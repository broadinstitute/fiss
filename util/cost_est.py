import iso8601, pytz, csv, os, subprocess
from firecloud import api as fapi
from collections import namedtuple, defaultdict
from collections.abc import Sequence
from datetime import datetime
from six import string_types

import fire

def get_vm_cost(machine_type, preemptible=True):
    """
    Cost per hour
    """
    preemptible_dict = {
        'n1-standard-1': 0.0100,  # 3.75 GB
        'n1-standard-2': 0.0200,  # 7.5 GB
        'n1-standard-4': 0.0400,  # 15  GB
        'n1-standard-8': 0.0800,  # 30  GB
        'n1-standard-16':0.1600,  # 60  GB
        'n1-standard-32':0.3200,  # 120 GB
        'n1-standard-64':0.6400,  # 240 GB
        'n1-highmem-2':  0.0250,  # 13  GB
        'n1-highmem-4':  0.0500,  # 26  GB
        'n1-highmem-8':  0.1000,  # 52  GB
        'n1-highmem-16': 0.2000,  # 104 GB
        'n1-highmem-32': 0.4000,  # 208 GB
        'n1-highmem-64': 0.8000,  # 416 GB
        'n1-highcpu-2':  0.0150,  # 1.80 GB
        'n1-highcpu-4':  0.0300,  # 3.60 GB
        'n1-highcpu-8':  0.0600,  # 7.20 GB
        'n1-highcpu-16': 0.1200,  # 14.40 GB
        'n1-highcpu-32': 0.2400,  # 28.80 GB
        'n1-highcpu-64': 0.4800,  # 57.6 GB
        'f1-micro':      0.0035,  # 0.6 GB
        'g1-small':      0.0070,  # 1.7 GB
    }

    standard_dict = {
        'n1-standard-1': 0.0475,
        'n1-standard-2': 0.0950,
        'n1-standard-4': 0.1900,
        'n1-standard-8': 0.3800,
        'n1-standard-16': 0.7600,
        'n1-standard-32': 1.5200,
        'n1-standard-64': 3.0400,
        'n1-highmem-2':  0.1184,
        'n1-highmem-4':  0.2368,
        'n1-highmem-8':  0.4736,
        'n1-highmem-16': 0.9472,
        'n1-highmem-32': 1.8944,
        'n1-highmem-64': 3.7888,
        'n1-highcpu-2':  0.0709,
        'n1-highcpu-4':  0.1418,
        'n1-highcpu-8':  0.2836,
        'n1-highcpu-16': 0.5672,
        'n1-highcpu-32': 1.1344,
        'n1-highcpu-64': 2.2688,
        'f1-micro':      0.0076,
        'g1-small':      0.0257,
    }

    if preemptible:
        return preemptible_dict[machine_type]
    else:
        return standard_dict[machine_type]

def get_storage(args):
    """
    Get total amount of storage used, in TB
    Pricing: $0.026/GB/month (multi-regional)
    $0.02/GB/month (regional)
    import os
    os.environ['CLOUDSDK_PYTHON'] = '/usr/local/var/pyenv/versions/2.7.12/bin/python'
    """
    r = fapi.get_workspace(args.project, args.workspace)
    assert r.status_code == 200
    r = r.json()
    s = subprocess.check_output('gsutil du -s gs://'+r['workspace']['bucketName'], shell=True, executable='/bin/bash')
    return float(s.decode().split()[0])/ 1024 ** 4

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
    r = fapi.list_submissions(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    statuses = sorted(r.json(), key=lambda k: k['submissionDate'], reverse=True)
    return statuses

def valid_status(x):
    """Firecloud API only returns complete records for 'Done' and 'Succeeded' """
    return (x['status'] == 'Done' and 'workflowStatuses' in x and
            'Succeeded' in x['workflowStatuses'] and x['workflowStatuses']['Succeeded'] == 1)

def fetch_filter_submissions(args, verbose=True):
    all_statuses = get_submissions(args)
    submissions  = []
    if len(all_statuses) > 0:
        # as a workflowdId is required for cost, filter for valid_status only
        valid_statuses = list(filter(valid_status, all_statuses))
        submissions = [0 for i in range(len(valid_statuses))]
        for i, submission in enumerate(valid_statuses):
            if verbose:
                print('\rFetching submissions {}/{}'.format(i+1, len(valid_statuses)), end='')
            rsp = fapi.get_submission(args.project, args.workspace, submission['submissionId'])
            assert rsp.status_code == 200
            r = rsp.json()
            submissions[i] = r
    return(submissions)


def fetch_metadata_submissions(args, submissions, verbose=True):
    metadata_status = defaultdict(list)
    for i, submission in enumerate(submissions):
        if verbose:
            print('\rFetching metadata {}/{}'.format(i+1, len(submissions)), end='')
        workflows = [w['workflowId'] for w in submission['workflows']]
        for w_id in workflows:
            metadata = fapi.get_workflow_metadata(args.project, args.workspace, submission['submissionId'], w_id)
            assert metadata.status_code == 200
            metadata_status[submission['submissionId']].append(metadata.json())
    return(metadata_status)

def cost2(submission, timezone='America/New_York'):
    """ Determine an estimated cost for submissions already run."""
    workflow_name = [sub['workflowName'] for sub in submission]

    tasks = [t['calls'].keys() for t in submission]
    task_names = [t.rsplit('.')[-1] for tk in tasks for t in tk]
    task_stats = defaultdict(list)

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
            else:
                #print('cached!')
                task_stats[task_name]['cached']       = True
                task_stats[task_name]['start_time']   = iso8601.parse_date(submission[0]['calls'][task_name][0]['start']).astimezone(pytz.timezone(timezone)).strftime('%H:%M')
                task_stats[task_name]['machine_type'] = 'cache-small'
                task_stats[task_name]['est_cost']     = 0

            #total_time_h = [workflow_time(task[t_attempt])/3600 for t_attempt in tasks]
            #task_stats[task_name]['total_time_h'] = sum(total_time_h) - sum(quota_time)

    return task_stats

def write_sec(sec_data, filename):
    """ Write submission_entity_costs to file """
    # split
    headers2 = set(['task','entity','method','submission_id'])
    for entity, subs_for_entity in sec_data.items():
        for sub_id, entry in subs_for_entity.items():
            for task in entry['cost']:
                headers2.update(list(entry['cost'][task].keys()))    
    
    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.DictWriter(csvfile, fieldnames=list(headers2), dialect=csv.excel_tab)
        csvwriter.writeheader()
        for entity, subs_for_entity in sec_data.items():
            for sub_id, entry in subs_for_entity.items():
                for task in entry['cost']:
                    task_entry = entry['cost'][task]
                    task_entry['task'] = task
                    task_entry['entity'] = entry['submission']['submissionEntity']['entityName']
                    task_entry['method'] = entry['submission']['methodConfigurationName']
                    task_entry['submission_id'] = entry['submission']['submissionId']
                    csvwriter.writerow(task_entry)

def main(project, workspace, output_file, verbose=True):
    VERBOSE = verbose

    Args = namedtuple('Args', 'project workspace')
    # args
    #args = Args(project='nci-mnoble-bi-org', workspace='GDAC_NGS_hg37')
    args = Args(project=project, workspace=workspace)

    submissions = fetch_filter_submissions(args, verbose=VERBOSE)

    if VERBOSE:
        print('\r' + ' ' * 120, end='', flush=True)

    metadata_status = fetch_metadata_submissions(args, submissions, verbose=VERBOSE)

    # group submissions by submission id
    submissions_by_id = {s['submissionId']:s for s in submissions}

    # group submission by entity id, then submission id
    submissions_by_entity = defaultdict(dict)
    for s in submissions:
        submissions_by_entity[s['submissionEntity']['entityName']][s['submissionId']] = s

    # group submission by entity id, then submission id, include cost calculations
    submissions_entity_cost = defaultdict(dict)
    for s in submissions:
        sub_cost = {'submission': s,
                    'cost': cost2(metadata_status[s['submissionId']])}
        submissions_entity_cost[s['submissionEntity']['entityName']][s['submissionId']] = sub_cost

    write_sec(submissions_entity_cost, output_file)


if __name__ == '__main__':
    fire.Fire(main)