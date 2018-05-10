"""
A simple FISS wrapper script to extract costing estimations from FireCloud workspaces.

Based on the version in dalmation (github.com/broadinstitute/dalmatian), with some minor
modifications to focus on 'sample-sets', rate-limit API requests, and remove dependency
on Pandas python library

The script includes a main for exectution via the command line, as an example:

python cost_to_tsv_simple.py --namespace broad-firecloud-cptac --workspace Philosopher_Test --verbose

but is as valuable in a REPL for less-than-whole-workspace costing profiles.
"""

import csv, os, subprocess, itertools, re, copy, random, argparse
from collections import namedtuple, defaultdict, Counter, OrderedDict
import collections
from datetime import datetime
from time import sleep
from pprint import pprint as pp

from firecloud import api as fapi
from firecloud.errors import *
import iso8601, pytz

from cost_storage import get_vm_cost

Args = namedtuple('Args', 'namespace workspace verbose use_timestamp')

DEFAULT_EXPORT_COLS = ['methodConfigurationName', 'call_workflow_name',
 'submissionEntity_entityName', 'status', 'est_cost', 'est_cost_dollars', 'attempts',
 'cached', 'submissionDate', 'statusLastChangedDate', 'start_time', 'end_time',
 'machine_type', 'time_h', 'time_h_minus_quota', 'time_m', 'methodConfigurationNamespace',
 'submissionEntity_entityType', 'submissionId', 'job_ids', 'submitter', 'workflowEntity_entityName',
 'workflowEntity_entityType', 'workflowId']


def convert_time(x):
    return datetime.timestamp(iso8601.parse_date(x))

def workflow_time(workflow):
    """
    Convert API output to timestamp difference
    """
    if 'end' in workflow:
        return convert_time(workflow['end']) - convert_time(workflow['start'])
    else:
        return 0

def get_submissions(args):
    r = fapi.list_submissions(args.namespace, args.workspace)
    fapi._check_response_code(r, 200)
    statuses = sorted(r.json(), key=lambda k: k['submissionDate'], reverse=True)
    return statuses

def valid_status(x):
    """Firecloud API only returns complete records for 'Done' and 'Succeeded' """
    return (x['status'] == 'Done' and 'workflowStatuses' in x and
            'Succeeded' in x['workflowStatuses'] and x['workflowStatuses']['Succeeded'] == 1)

def filter_submissions(submissions, known_submissions_files):
    """ Filter submissions according to known_submissions.
        Exclude recalculating cost metrics for already accumulated values. """
    known_submissions = []
    for filename in known_submissions_files:
        # load as series of OrderedDicts
        with open(test_file) as csvfile:
            reader = csv.DictReader(csvfile, delimiter="\t")
            keys = reader.fieldnames
            r = csv.reader(csvfile, delimiter="\t")
            x = [OrderedDict(zip(keys, row)) for row in r]
            known_submissions.append(x)

    prior_sub_collection = {s['submissionId'] for s in x for x in known_submissions}
    new_submissions = []
    for submission in submissions:
        if submission['submissionId'] not in prior_sub_collection:
            new_submissions.append(submission)

    return new_submissions


def process_attempt(attempt):
    rsp = {}
    rsp['preemptible']   = attempt.get('preemptible', None)
    rsp['start']         = attempt.get('start', None)
    rsp['end']           = attempt.get('end', None)
    rsp['cache']         = any(['hit' in attempt['callCaching'] and attempt['callCaching']['hit']])
    rsp['workflow_time'] = workflow_time(rsp) / 3600
    rsp['backend_status'] = attempt.get('backendStatus', None)
    try:
        machine = attempt['jes']['machineType'].rsplit('/')[-1]
    except KeyError:
        machine = None
    rsp['machine'] = machine
    return rsp

def fetch_submissions(args, target_submissions, kind=0.3):
    # as a workflowdId is required for cost, filter for valid_status only
    submissions = [0 for i in range(len(target_submissions))]
    errors = {}
    for i, submission in enumerate(target_submissions):
        if args.verbose:
            print('\rFetching submissions {}/{}'.format(i+1, len(target_submissions)), end='')
        try:
            rsp = fapi.get_submission(args.namespace, args.workspace, submission['submissionId'])
            fapi._check_response_code(rsp, (200, ))
            r = rsp.json()
            submissions[i] = r
        except FireCloudServerError as fcse:
            errors[submission['submissionId']] = {'index': i, 'submission': submission, 'error': fcse}
        if kind:
            sleep(random.uniform(0.1, kind))
    return submissions, errors

def fetch_metadata_submissions(args, submissions, kind=0.3):
    metadata_status = defaultdict(list)
    missing_workflows = defaultdict(list)
    for i, submission in enumerate(submissions):
        if args.verbose:
            print('\rFetching metadata {}/{}'.format(i+1, len(submissions)), end='')
        try:
            workflows = [w['workflowId'] for w in submission['workflows']]
            for w_id in workflows:
                metadata = fapi.get_workflow_metadata(args.namespace, args.workspace, submission['submissionId'], w_id)
                if metadata.status_code == 200:
                    metadata_status[submission['submissionId']].append(metadata.json())
                else:
                    metadata_status[submission['submissionId']].append({'status_code': metadata.status_code })
        except KeyError:
            missing_workflows[submission['submissionId']].append(submission)
        except FireCloudServerError as fsce:
            missing_workflows[submission['submissionId']].append(submission)
        if kind:
            sleep(random.uniform(0.1, kind))

    return metadata_status, missing_workflows

def submission_cost(submission, timezone='America/New_York'):
    """ Determine runtime attributes for submissions already run."""
    workflow_name = [sub['workflowName'] for sub in submission]

    tasks = [t['calls'].keys() for t in submission]
    task_names = [t.rsplit('.')[-1] for tk in tasks for t in tk]
    task_stats = defaultdict(list)
    runtime_stats = defaultdict(list)
    if len(submission) == 0:
        return {}, [], {'submission': submission, 'workflow_name': workflow_name}

    errors = {}

    for task_name, task in submission[0]['calls'].items():
        task_stats[task_name] = {}
        for i, t in enumerate(task):
            successes = {}
            preemptions = []
            if 'shardIndex' in t:
                scatter = True
                if t['shardIndex'] in successes:
                    preemptions.append(t)
                successes[t['shardIndex']] = t  # last shard (assume success follows preemptions)
            else:
                scatter = False
                successes[0] = t
                preemptions = []

            task_stats[task_name]['time_h'] = sum([workflow_time(j)/3600 for j in successes.values()])
            task_stats[task_name]['time_m'] = sum([workflow_time(j)/60 for j in successes.values()])
            quota_time = [e for m in successes.values() for e in m['executionEvents'] if e['description']=='waiting for quota']
            quota_time = [(convert_time(q['endTime']) - convert_time(q['startTime']))/3600 for q in quota_time]
            task_stats[task_name]['time_h_minus_quota'] = task_stats[task_name]['time_h'] - sum(quota_time)

            total_time_h = [workflow_time(t_attempt)/3600 for t_attempt in submission[0]['calls'][task_name]]
            if not any(['hit' in j['callCaching'] and j['callCaching']['hit'] for j in submission[0]['calls'][task_name]]):
                task_stats[task_name]['cached'] = False
                was_preemptible = [j['preemptible'] for j in submission[0]['calls'][task_name]]
                if len(preemptions) > 0:
                    assert was_preemptible[0]
                    task_stats[task_name]['max_preempt_time_h'] = max([workflow_time(t_attempt) for t_attempt in preemptions])/3600
                task_stats[task_name]['attempts'] = len(submission[0]['calls'][task_name])
                task_stats[task_name]['start_time'] = iso8601.parse_date(submission[0]['calls'][task_name][0]['start']).astimezone(pytz.timezone(timezone)).strftime('%H:%M')
                task_stats[task_name]['end_time']   = iso8601.parse_date(submission[0]['calls'][task_name][0]['end']).astimezone(pytz.timezone(timezone)).strftime('%H:%M')
                try:
                    machine_types = [j['jes']['machineType'].rsplit('/')[-1] for j in submission[0]['calls'][task_name]]
                    task_stats[task_name]['machine_type'] = machine_types[-1]  # use last instance
                    task_stats[task_name]['est_cost'] = sum([get_vm_cost(m,p)*h for h,m,p in zip(total_time_h, machine_types, was_preemptible)])
                    task_stats[task_name]['est_cost_dollars'] = round(task_stats[task_name]['est_cost'], 2)
                except KeyError:
                    # missing machineType keyword, cached subtask
                    pass
                task_stats[task_name]['job_ids'] = ','.join([j['jobId'] for j in successes.values()])
                runtime_stats[task_name].append([process_attempt(i) for i in submission[0]['calls'][task_name]])
            else:
                task_stats[task_name]['cached']       = True
                task_stats[task_name]['start_time']   = iso8601.parse_date(submission[0]['calls'][task_name][0]['start']).astimezone(pytz.timezone(timezone)).strftime('%H:%M')
                task_stats[task_name]['machine_type'] = 'cache-small'
                runtime_stats[task_name].append(task)
    return task_stats, runtime_stats, errors

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_sub(sub, additional_core_fields=[], exclude=('inputResolutions','messages')):
    '''extract core fields from submission record'''
    result = {}
    sub = copy.copy(sub)
    core_fields = {'methodConfigurationName', 'methodConfigurationNamespace',
                   'status', 'submissionDate', 'submissionEntity', 'submissionId',
                   'submitter', 'status'}
    fields = core_fields.union(additional_core_fields)
    for k in fields:
        v = sub[k]
        if isinstance(v, dict):
            v = flatten({k:v})
            result.update(v)
        else:
            result[k] = v

    for x in sub['workflows']:
        for e in exclude:
            x.pop(e, None)
        x = flatten(x)    
        result.update(x)
    return result

def set_u(seq):
    """ Filter a sequence into a unique set while maintaining order """
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def write_sec_tasks(sec_data, filename, column_order=DEFAULT_EXPORT_COLS, dialect=csv.excel_tab):
    """ Write submission_entity_costs to file """
    headers = list()

    for entity, subs_for_entity in sec_data.items():
        for sub_id, entries in subs_for_entity.items():
            for entry in entries:
                headers.extend(list(entry.keys()))
    
    headers = set_u(headers)

    if column_order:
        column_set = set(column_order)
        seen_add = column_set.add
        headers = [x for x in column_order if x in column_set]
        header_missing = [x for x in column_order if x not in column_set]
        headers.extend(header_missing)

    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.DictWriter(csvfile, fieldnames=list(headers), dialect=dialect)
        csvwriter.writeheader()
        for entity, subs_for_entity in sec_data.items():
            for sub_id, entries in subs_for_entity.items():
                for entry in entries:
                    csvwriter.writerow(entry)

def fn_slug(*args):
    value = '_'.join(map(str, args))
    value = re.sub(r'[^\w\s-]', '', value).strip().lower()
    return re.sub(r'[-\s]+', '_', value)

def compute_cost(args, filter_sub_targets=None):
    # test_file = "broad_firecloud_gdac_analyses__2018_01_18__2018_03_19_13_20_15.tsv"
    # cost_output_name
    
    try: 
        all_submissions = get_submissions(args)
    except FireCloudServerError as fcse:
        pp(fcse.args)
    
    if len(all_submissions) == 0:
        print("[ERROR] No FireCloud submissions to retrieve for {}/{}".format(args.namespace, args.workspace))
    
    # group submissions by status
    subs_by_status = defaultdict(list)
    for sub in all_submissions:
        subs_by_status[sub['status']].append(sub)
    
    subs_stats = {k: len(subs_by_status[k]) for k in subs_by_status}
    
    sub_status_targets = ('Aborted', 'Done')
    
    sub_targets = [v for k, v in subs_by_status.items() if k in sub_status_targets ]
    
    sub_targets = list(itertools.chain.from_iterable(sub_targets))
    
    if filter_sub_targets:
        # filter requested submissions 
        if not isinstance(filter_sub_targets, list):
            filter_subs = [filter_sub_targets]
        else:
            filter_subs = filter_sub_targets
        new_sub_targets = filter_submissions(sub_targets, filter_subs)
    else:
        new_sub_targets = sub_targets
    
    
    submissions, errors = fetch_submissions(args, new_sub_targets, kind=0.8)
    
    if errors:
        print('\r' + ' ' * 120, end='', flush=True)
        print('\rErrors retrieving submissions: {} errors'.format(len(errors.keys())))
    
    print('\r' + ' ' * 120, end='', flush=True)

    sub_metadata, missing_workflows = fetch_metadata_submissions(args, submissions, kind=0.8)
    
    if len(missing_workflows) != 0:
        print('\r' + ' ' * 120, end='', flush=True)
        print('\rMissing workflows: {} submissions'.format(len(missing_workflows)))
        if args.verbose:
            # collect method configs
            mw = [v['methodConfigurationName'] for key, value in missing_workflows.items() for v in value ]
            pp(Counter(mw))
    
    sub_stats = defaultdict(dict)
    failed_stats = defaultdict(dict)
    for index, s in enumerate(submissions):
        if args.verbose:
            print('\rCalculating runtime attrs {}/{}'.format(index+1, len(submissions)), end='')
        sub_set_status = {s2['status'] for s2 in s['workflows']}
        if 'Failed' in sub_set_status:
            failed_stats[s['submissionId']] = {'submission': s}
            continue
        task_calls, runtime_stats, errors = submission_cost(sub_metadata[s['submissionId']])
        sub_calls = {'submission': s,
                     'calls': task_calls,
                     'runtime_stats': runtime_stats}
        sub_stats[s['submissionId']] = sub_calls
    
    if failed_stats:
        print('\r' + ' ' * 120, end='', flush=True)
        print('\rFailed workflows: {} submissions'.format(len(failed_stats)))
        if args.verbose:
            fm = [v['methodConfigurationName'] for key, value in failed_stats.items() for k, v in value.items()]
            pp(Counter(fm))


    def merge_dicts(d1, d2):
        d3 = d1.copy()   
        d3.update(d2)
        return d3

    submissions_entity_cost = dict()
    for submission_id in sub_stats:
        try:
            sub  = sub_stats[submission_id]['submission']
        except KeyError:
            continue
        sub_flat = flatten_sub(sub)
        submission_record = []
        calls = sub_stats[submission_id]['calls']
        for name, call in calls.items():
            # call is a dict
            call_task = copy.copy(call)
            sub_flat_copy = copy.copy(sub_flat)
            call_task['call_workflow_name'] = name
            task_flat = merge_dicts(sub_flat_copy, call_task)
            submission_record.append(task_flat)

        if sub['submissionEntity']['entityName'] not in submissions_entity_cost:
            submissions_entity_cost[sub['submissionEntity']['entityName']] = dict()

        submissions_entity_cost[sub['submissionEntity']['entityName']][sub_flat['submissionId']] = submission_record
    
    cost_output_slug = fn_slug(args.namespace, args.workspace)
    
    if args.use_timestamp:
        time_label = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        cost_output_slug = cost_output_slug + "__" + time_label
    
    cost_output_name = cost_output_slug + '.tsv'
    
    write_sec_tasks(submissions_entity_cost, cost_output_name)

    return cost_output_name

def main_repl(namespace, workspace, verbose=True, use_timestamp=True):
    args = Args(namespace=namespace, workspace=workspace, verbose=verbose, use_timestamp=use_timestamp)


def main():
    parser = argparse.ArgumentParser(description='Collect billing metrics from FireCloud workspace')

    parser.add_argument('--namespace', type=str, help='FireCloud namespace')
    parser.add_argument('--workspace', type=str, help='FireCloud workspace')
    parser.add_argument('--filter', nargs="*", help="A file, dir, glob pattern to filter submissions")
    parser.add_argument('--verbose', action='store_true', help='print verbose messages')
    parser.add_argument('--use_timestamp', action='store_true', default=True, help='print messages')

    args = parser.parse_args()
    
    if args.filter:
        full_paths = [os.path.join(os.getcwd(), path) for path in args.filter]
        filter_sub_targets = set()
        for path in full_paths:
            if os.path.isfile(path):
                filter_sub_targets.add(path)
    else:
        filter_sub_targets = None
    
    cost_file = compute_cost(args, filter_sub_targets=filter_sub_targets)

    if args.verbose:
        print("Wrote cost file to {}".format(cost_file))

    return 0

if __name__ == '__main__':
    main()


   
