from six import print_
import time
import pydot

from firecloud import api as fapi

def supervise(args):
    """ Supervise submission of jobs from a Firehose-style workflow of workflows"""
    # Get arguments
    workspace = args.workspace
    project = args.project
    api_url = args.api_url
    namespace = args.namespace
    entity = args.entity


    # Parse workflow description
    with open(args.workflow) as wf:
        graph_data = wf.read()

    graph = pydot.graph_from_dot_data(graph_data)[0]
    nodes = graph.get_nodes()

    # Make the lookup of each node easier
    node_dict = {n.get_name(): n for n in nodes}

    # Initialize empty list of dependencies
    for n in node_dict:
        node_dict[n].dependencies = []
        node_dict[n].monitor_state = "Not Started"
        node_dict[n].evaluated = False
        node_dict[n].succeeded = False

    edges = graph.get_edges()

    # Iterate over the edges, and get the dependency information for each node
    for e in edges:
        source = e.get_source()
        dest = e.get_destination()

        dep = e.get_attributes()
        dep['workflow'] = source

        node_dict[dest].dependencies.append(dep)

    # Monitor loop. Keep going until all nodes have been evaluated
    while not all(node_dict[n].evaluated for n in node_dict):
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


        for n in node_dict:
            task = node_dict[n]

            if task.monitor_state == "Not Started":
                # See if all of the dependencies have been satisfied
                all_evaluated = True
                for dep in node_dict[n].dependencies:
                    dep_node = node_dict[dep['workflow']]
                    if not dep_node.evaluated:
                        all_evaluated = False

                # if all of the dependencies have been evaluated, we can evaluate
                # this node
                if all_evaluated:
                    # Now check the satisfied Mode of all the dependencies
                    should_run = True
                    for dep in node_dict[n].dependencies:
                        dep_node = node_dict[dep['workflow']]
                        mode = dep['satisfiedMode']

                        # Task must have succeeded for OnComplete
                        if mode == '"OnComplete"' and not dep_node.succeeded:
                            should_run = False
                        # 'Always' and 'Optional' run once the deps have been
                        # evaluated

                    if should_run:
                        # Submit the workflow to FC
                        fc_config = n.strip('"')
                        #TODO: replace with better logging
                        print_("Starting workflow " + fc_config )
                        r = fapi.create_submission(
                            project, workspace, namespace, fc_config,
                            entity, etype="sample_set", expression=None,
                            api_root=api_url
                        )

                        #TODO: Error handling
                        # save the submission_id so we can poll the result later
                        sub_id = r.json()['submissionId']
                        task.submissionId = sub_id
                        task.monitor_state = "Running"
                        running += 1

                    else:
                        # This task will never be able to run, mark evaluated
                        task.monitor_state = "Evaluated"
                        task.evaluated = True
                        completed += 1
                else:
                    waiting += 1


            elif task.monitor_state == "Running":
                # Check the progress of the submission
                r = fapi.get_submission(
                    project, workspace, task.submissionId, api_root=api_url
                )

                # Check the submission status, look for "Done"
                body = r.json()
                status = body['status']
                if status == "Done":
                    # Look at the individual workflows to see if there were
                    # failures
                    print_("Workflow " + n + " completed")
                    success = not any(w['status'] == 'Failed' for w in body['workflows'])
                    task.evaluated = True
                    task.succeeded = success
                    task.monitor_status = "Completed."

                    completed += 1
                else:
                    running += 1

                # If the submission isn't done, do nothing

        # Sleep, wait for changes
        print_("{0} Waiting, {1} Running, {2} Completed".format(waiting, running, completed))
        time.sleep(20)
