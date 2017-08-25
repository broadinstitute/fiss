import time
import pydot
import logging
import json

from firecloud import api as fapi

logging.basicConfig(format='%(asctime)s::%(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %I:%M:%S', level=logging.INFO)


def supervise(project, workspace, namespace, workflow,
              sample_sets, recovery_file):
    """ Supervise submission of jobs from a Firehose-style workflow of workflows"""
    # Get arguments

    logging.info("Initializing FireCloud Supervisor...")
    logging.info("Saving recovery checkpoints to " + recovery_file)

    # Parse workflow description
    # these three objects must be saved in order to recover the supervisor

    args = {
        'project'  : project,
        'workspace': workspace,
        'namespace': namespace,
        'workflow' : workflow,
        'sample_sets': sample_sets
    }

    monitor_data, dependencies = init_supervisor_data(workflow, sample_sets)
    recovery_data = {
        'args' : args,
        'monitor_data' : monitor_data,
        'dependencies' : dependencies
    }

    # Monitor loop. Keep going until all nodes have been evaluated
    supervise_until_complete(monitor_data, dependencies, args, recovery_file)


def init_supervisor_data(dotfile, sample_sets):
    """ Parse a workflow description written in DOT (like Firehose)"""
    with open(dotfile) as wf:
        graph_data = wf.read()

    graph = pydot.graph_from_dot_data(graph_data)[0]
    nodes = [n.get_name().strip('"') for n in graph.get_nodes()]

    monitor_data = dict()
    dependencies = {n:[] for n in nodes}

    # Initialize empty list of dependencies
    for n in nodes:
        monitor_data[n] = dict()
        for sset in sample_sets:
            monitor_data[n][sset] = {
                'state'        : "Not Started",
                'evaluated'    : False,
                'succeeded'    : False
            }

    edges = graph.get_edges()

    # Iterate over the edges, and get the dependency information for each node
    for e in edges:
        source = e.get_source().strip('"')
        dest = e.get_destination().strip('"')

        dep = e.get_attributes()
        dep['upstream_task'] = source

        dependencies[dest].append(dep)

    return monitor_data, dependencies


def validate_monitor_tasks(dependencies, args):
    """ Validate that all entries in the supervisor are valid task configurations and
        that all permissions requirements are satisfied.
    """
    # Make a list of all task configurations needed to supervise
    sup_configs = sorted(dependencies.keys())


    try:
        logging.info("Validating supervisor data...")
        # Make an api call to list task configurations in the workspace
        r = fapi.list_workspace_configs(args['project'], args['workspace'])
        fapi._check_response_code(r, 200)
        space_configs = r.json()

        # Make a dict for easy lookup later
        space_configs = { c["name"]: c for c in space_configs}

        # Also make an api call to list methods you have view permissions for
        r = fapi.list_repository_methods()
        fapi._check_response_code(r, 200)
        repo_methods = r.json()

        ## Put in a form that is more easily searchable: namespace/name:snapshot
        repo_methods = {m['namespace'] + '/' + m['name'] + ':' + str(m['snapshotId'])
                        for m in repo_methods if m['entityType'] == 'Workflow'}

        valid = True

        for config in sup_configs:
            # ensure config exists in the workspace
            if config not in space_configs:
                logging.error("No task configuration for "
                              + config + " found in " + args['project'] + "/"
                              + args['workspace'])
                valid = False
            else:
                # Check access permissions for the referenced method
                m = space_configs[config]['methodRepoMethod']
                ref_method = m['methodNamespace'] + "/" + m['methodName'] + ":" + str(m['methodVersion'])
                if ref_method not in repo_methods:
                    logging.error(config+ " -- You don't have permisson to run the referenced method: "
                                  + ref_method)
                    valid = False

    except Exception as e:
        logging.error("Exception occurred while validating supervisor: " + str(e))
        raise
        return False

    return valid

def recover_and_supervise(recovery_file):
    """ Retrieve monitor data from recovery_file and resume monitoring """
    try:
        logging.info("Attempting to recover Supervisor data from " + recovery_file)
        with open(recovery_file) as rf:
            recovery_data = json.load(rf)
            monitor_data = recovery_data['monitor_data']
            dependencies = recovery_data['dependencies']
            args = recovery_data['args']

    except:
        logging.error("Could not recover monitor data, exiting...")
        return 1

    logging.info("Data successfully loaded, resuming Supervisor")
    supervise_until_complete(monitor_data, dependencies, args, recovery_file)


def supervise_until_complete(monitor_data, dependencies, args, recovery_file):
    """ Supervisor loop. Loop forever until all tasks are evaluated or completed """
    project = args['project']
    workspace = args['workspace']
    namespace = args['namespace']
    sample_sets = args['sample_sets']
    recovery_data = {'args': args}

    if not validate_monitor_tasks(dependencies, args):
        logging.error("Errors found, aborting...")
        return

    while True:
        # There are 4 possible states for each node:
        #   1. Not Started -- In this state, check all the dependencies for the
        #         node (possibly 0). If all of them have been evaluated, and the
        #         satisfiedMode is met, start the task, change to "Running". if
        #         satisfiedMode is not met, change to "Evaluated"
        #
        #   2. Running -- Submitted in FC. Check the submission endpoint, and
        #         if it has completed, change to "Completed", set evaluated=True,
        #         and whether the task succeeded
        #         Otherwise, do nothing
        #
        #   3. Completed -- Job ran in FC and either succeeded or failed. Do nothing
        #   4. Evaluated -- All dependencies evaluated, but this task did not run
        #         do nothing

        # Keep a tab of the number of jobs in each category
        running = 0
        waiting = 0
        completed = 0

        # Get the submissions
        r = fapi.list_submissions(project, workspace)
        sub_list = r.json()
        #TODO: filter this list by submission time first?
        sub_lookup = {s["submissionId"]: s for s in sub_list}

        # Keys of dependencies is the list of tasks to run
        for n in dependencies:
            for sset in sample_sets:
                task_data = monitor_data[n][sset]
                if task_data['state'] == "Not Started":
                    # See if all of the dependencies have been evaluated
                    upstream_evaluated = True
                    for dep in dependencies[n]:
                        # Look up the status of the task
                        upstream_task_data = monitor_data[dep['upstream_task']][sset]
                        if not upstream_task_data.get('evaluated'):
                            upstream_evaluated = False

                    # if all of the dependencies have been evaluated, we can evaluate
                    # this node
                    if upstream_evaluated:
                        # Now check the satisfied Mode of all the dependencies
                        should_run = True
                        for dep in dependencies[n]:
                            upstream_task_data = monitor_data[dep['upstream_task']][sset]
                            mode = dep['satisfiedMode']

                            # Task must have succeeded for OnComplete
                            if mode == '"OnComplete"' and not upstream_task_data['succeeded']:
                                should_run = False
                            # 'Always' and 'Optional' run once the deps have been
                            # evaluated

                        if should_run:
                            # Submit the workflow to FC
                            fc_config = n
                            logging.info("Starting workflow " + fc_config + " on " + sset)

                            # How to handle errors at this step?
                            for retry in range(3):
                                r = fapi.create_submission(
                                    project, workspace, namespace, fc_config,
                                    sset, etype="sample_set", expression=None
                                )
                                if r.status_code == 201:
                                    task_data['submissionId'] = r.json()['submissionId']
                                    task_data['state'] = "Running"
                                    running += 1
                                    break
                                else:
                                    # There was an error, under certain circumstances retry
                                    logging.debug("Create_submission for " + fc_config
                                                  + "failed on " + sset + " with the following response:"
                                                  + r.content + "\nRetrying...")

                            else:
                                # None of the attempts above succeeded, log an error, mark as failed
                                logging.error("Maximum retries exceeded")
                                task_data['state'] = 'Completed'
                                task_data['evaluated'] = True
                                task_data['succeeded'] = False

                        else:
                            # This task will never be able to run, mark evaluated
                            task_data['state'] = "Evaluated"
                            task_data['evaluated'] = True
                            completed += 1
                    else:
                        waiting += 1

                elif task_data['state'] == "Running":
                    submission = sub_lookup[task_data['submissionId']]
                    status = submission['status']

                    if status == "Done":
                        # Look at the individual workflows to see if there were
                        # failures
                        logging.info("Workflow " + n + " completed for " + sset)
                        success = 'Failed' not in submission['workflowStatuses']
                        task_data['evaluated'] = True
                        task_data['succeeded'] = success
                        task_data['state'] = "Completed"

                        completed += 1
                    else:
                        # Submission isn't done, don't do anything
                        running += 1

                else:
                    # Either Completed or evaluated
                    completed += 1

                # Save the state of the monitor for recovery purposes
                # Have to do this for every workflow + sample_set so we don't lose track of any
                recovery_data['monitor_data'] = monitor_data
                recovery_data['dependencies'] = dependencies

                with open(recovery_file, 'w') as rf:
                    json.dump(recovery_data, rf)

        logging.info("{0} Waiting, {1} Running, {2} Completed".format(waiting, running, completed))

        # If all tasks have been evaluated, we are done
        if all(monitor_data[n][sset]['evaluated']
                      for n in monitor_data for sset in monitor_data[n]):
            logging.info("DONE.")
            break
        time.sleep(30)
