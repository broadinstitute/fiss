#! /usr/bin/env python
"""
FISS -- (Fi)reCloud (S)ervice (Selector)

This module provides a command line interface to FireCloud
For more details see https://software.broadinstitute.org/firecloud/
"""
import json
import sys
import os
from inspect import getsourcelines
import argparse
import subprocess

from six import print_, iteritems, string_types, itervalues
from six.moves import input
from yapsy.PluginManager import PluginManager

from firecloud import api as fapi
from firecloud.errors import *
from firecloud.__about__ import __version__

PLUGIN_PLACES = ["plugins", os.path.expanduser('~/.fiss/plugins')]

def fiss_cmd(function):
    """ Decorator to indicate a function is a FISS command """
    function.fiss_cmd = True
    return function

#################################################
# SubCommands
#################################################
@fiss_cmd
def space_list(args):
    """ List available workspaces. """
    r = fapi.list_workspaces(args.api_url)
    fapi._check_response_code(r, 200)

    #Parse the JSON for the workspace + namespace
    workspaces = r.json()
    pretty_spaces = []
    for space in workspaces:
        ns = space['workspace']['namespace']
        ws = space['workspace']['name']
        pretty_spaces.append(ns + '\t' + ws)

    #Sort for easier viewing, ignore case
    pretty_spaces = sorted(pretty_spaces, key=lambda s: s.lower())
    for s in pretty_spaces:
        print_(s)


@fiss_cmd
def space_lock(args):
    """  Lock a workspace. """
    r = fapi.lock_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 204)
    print_('Locked workspace {0}/{1}'.format(args.project, args.workspace))


@fiss_cmd
def space_new(args):
    """ Create a new workspace. """
    r = fapi.create_workspace(args.project, args.workspace,
                                 args.protected, dict(), args.api_url)
    fapi._check_response_code(r, 201)
    print_('Created workspace {0}/{1}'.format(args.project, args.workspace))
    print_(r.content)


@fiss_cmd
def space_info(args):
    """ Get metadata for a workspace. """
    r = fapi.get_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)

    #TODO?: pretty_print_workspace(c)
    print_(r.content)


@fiss_cmd
def space_delete(args):
    """ Delete a workspace. """
    message = "WARNING: this will delete workspace: \n\t{0}/{1}".format(
        args.project, args.workspace
    )
    if not args.yes and not _confirm_prompt(message):
        #Don't do it!
        return

    r = fapi.delete_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 202)
    print_('Deleted workspace {0}/{1}'.format(args.project, args.workspace))


@fiss_cmd
def space_unlock(args):
    """ Unlock a workspace. """
    r = fapi.unlock_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 204)
    print_('Unlocked workspace {0}/{1}'.format(args.project, args.workspace))


@fiss_cmd
def space_clone(args):
    """ Clone a workspace """
    if not args.to_workspace:
        args.to_workspace = args.workspace
    if not args.to_project:
        args.to_project = args.project
    if (args.project == args.to_project
        and args.workspace == args.to_workspace):
        eprint("Error: destination project and namespace must differ from"
               " cloned workspace")
        return 1

    r = fapi.clone_workspace(
        args.project, args.workspace,
        args.to_project, args.to_workspace, args.api_url
    )
    fapi._check_response_code(r, 201)
    msg =  args.project + '/' + args.workspace
    msg += " successfully cloned to " + args.to_project
    msg += "/" + args.to_workspace
    print_(msg)


@fiss_cmd
def entity_import(args):
    """ Upload an entity loadfile. """
    r = fapi.upload_entities_tsv(args.project, args.workspace,
                                 args.tsvfile, args.api_url)
    fapi._check_response_code(r, [200, 201])
    print_('Successfully uploaded entities')


@fiss_cmd
def entity_types(args):
    """ List entity types in a workspace. """
    r = fapi.list_entity_types(args.project, args.workspace,
                               args.api_url)
    fapi._check_response_code(r, 200)
    for etype in r.json():
        print_(etype)


@fiss_cmd
def entity_list(args):
    """ List entities in a workspace. """
    r = fapi.get_entities_with_type(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)
    for entity in r.json():
        print_('{0}\t{1}'.format(entity['entityType'], entity['name']))


# REMOVED: This now returns a *.zip* file containing two tsvs, which is far
# less useful for FISS users...
# def entity_tsv(args):
#     """ Get list of entities in TSV format. """
#     r = fapi.get_entities_tsv(args.project, args.workspace,
#                               args.entity_type, args.api_url)
#     fapi._check_response_code(r, 200)
#
#     print_(r.content)

@fiss_cmd
def participant_list(args):
    """ List participants in a workspace. """
    r = fapi.get_entities(args.project, args.workspace,
                          "participant", args.api_url)
    fapi._check_response_code(r, 200)
    for entity in r.json():
        print_(entity['name'])


@fiss_cmd
def sample_list(args):
    """ List samples in a workspace. """
    r = fapi.get_entities(args.project, args.workspace,
                             "sample", args.api_url)
    fapi._check_response_code(r, 200)
    for entity in r.json():
        print_(entity['name'])


@fiss_cmd
def sset_list(args):
    """ List sample sets in a workspace """
    r = fapi.get_entities(args.project, args.workspace,
                          "sample_set", args.api_url)
    fapi._check_response_code(r, 200)

    for entity in r.json():
        print_(entity['name'])


@fiss_cmd
def entity_delete(args):
    """ Delete entity in a workspace. """
    raise NotImplementedError("Entity deletion is currently broken in FC :(")

    prompt = "WARNING: this will delete {0} {1} in {2}/{3}".format(
        args.entity_type, args.entity, args.project, args.workspace
    )
    if not args.yes and not _confirm_prompt(prompt):
        #Don't do it!
        return
    r = fapi.delete_entity(args.project, args.workspace,
                           args.entity_type, args.entity, args.api_url)
    fapi._check_response_code(r, 204)
    print_("Succesfully deleted " + args.type + " " + args.entity)


@fiss_cmd
def participant_delete(args):
    args.entity_type = "participant"
    return entity_delete(args)


@fiss_cmd
def sample_delete(args):
    args.entity_type = "sample"
    return entity_delete(args)


@fiss_cmd
def sset_delete(args):
    args.entity_type = "sample_set"
    return entity_delete(args)


@fiss_cmd
def space_acl(args):
    """ Get Access Control List for a workspace."""
    r = fapi.get_workspace_acl(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)
    for user, role in iteritems(r.json()):
        print_('{0}\t{1}'.format(user, role))


@fiss_cmd
def space_set_acl(args):
    """ Assign an ACL role to list of users for a workspace """
    acl_updates = [{"email": user,
                   "accessLevel": args.role} for user in args.users]
    r = fapi.update_workspace_acl(args.project, args.workspace,
                                  acl_updates, args.api_url)
    fapi._check_response_code(r, 200)
    print_("Successfully updated {0} role(s)".format(len(acl_updates)))


@fiss_cmd
def flow_new(args):
    """ Submit a new workflow to the methods repository. """
    r = fapi.update_repository_method(args.namespace, args.method, args.synopsis,
                                      args.wdl, args.doc, args.api_url)
    fapi._check_response_code(r, 201)
    print_("Successfully pushed {0}/{1}".format(args.namespace, args.method))


@fiss_cmd
def flow_delete(args):
    """ Redact a workflow in the methods repository """
    message = "WARNING: this will delete workflow \n\t{0}/{1}:{2}".format(
        args.namespace, args.method, args.snapshot_id
    )
    if not args.yes and not _confirm_prompt(message):
        #Don't do it!
        return
    r = fapi.delete_repository_method(args.namespace, args.method,
                                      args.snapshot_id, args.api_url)
    fapi._check_response_code(r, 200)
    print_("Successfully redacted workflow.")


@fiss_cmd
def flow_acl(args):
    """ Get Access Control List for a workflow """
    r = fapi.get_repository_method_acl(args.namespace, args.method,
                                       args.snapshot_id, args.api_url)
    fapi._check_response_code(r, 200)
    for d in r.json():
        user = d['user']
        role = d['role']
        print_('{0}\t{1}'.format(user, role))


@fiss_cmd
def flow_set_acl(args):
    """ Assign an ACL role to a list of users for a worklow. """
    acl_updates = [{"user": user, "role": args.role} for user in args.users]
    r = fapi.update_repository_method_acl(args.namespace, args.method,
                                          args.snapshot_id, acl_updates,
                                          args.api_url)
    fapi._check_response_code(r, 200)
    print_("Successfully set method acl")


@fiss_cmd
def flow_list(args):
    """ List workflows in the methods repository """
    r = fapi.list_repository_methods(args.api_url)
    fapi._check_response_code(r, 200)

    #Parse the JSON for the workspace + namespace
    methods = r.json()
    results = []
    for m in methods:
        ns = m['namespace']
        n = m['name']
        sn_id = m['snapshotId']
        results.append('{0}\t{1}\t{2}'.format(ns,n,sn_id))

    #Sort for easier viewing, ignore case
    results = sorted(results, key=lambda s: s.lower())
    for r in results:
        print_(r)


@fiss_cmd
def config_list(args):
    """ List configurations in the methods repository or a workspace. """
    if args.workspace:
        # Get methods from a specific workspace
        if not args.project:
            eprint("No project provided and no DEFAULT_PROJECT found")
            return 1
        r = fapi.list_workspace_configs(args.project,
                                        args.workspace, args.api_url)
        fapi._check_response_code(r, 200)
    else:
        # Get methods from the repository
        r = fapi.list_repository_configs(args.api_url)
        fapi._check_response_code(r, 200)

    #Parse the JSON for the workspace + namespace
    methods = r.json()
    results = []
    for m in methods:
         ns = m['namespace']
         n = m['name']
         # Use the get syntax here since workspace configs have no snapshot_id
         sn_id = m.get('snapshotId', "")
         results.append('{0}\t{1}\t{2}'.format(ns,n,sn_id))

    #Sort for easier viewing, ignore case
    results = sorted(results, key=lambda s: s.lower())
    for r in results:
        print_(r)


@fiss_cmd
def config_acl(args):
    """ Get Access Control List for a method configuration. """
    r = fapi.get_repository_config_acl(args.namespace, args.config,
                                       args.snapshot_id, args.api_url)
    fapi._check_response_code(r, 200)
    for d in r.json():
        user = d['user']
        role = d['role']
        print_('{0}\t{1}'.format(user, role))


@fiss_cmd
def attr_get(args):
    """ Get attributes from entities or workspaces. """
    ##if entities was specified
    if args.entity_type is not None:
        entities = _entity_paginator(args.project, args.workspace,
                                     args.entity_type,
                                     page_size=1000, filter_terms=None,
                                     sort_direction="asc", api_root=args.api_url)

        attr_list = args.attributes
        if not attr_list:
            # Get a set of all available attributes, then sort them
            attr_list = {k for e in entities for k in e['attributes'].keys()}
            attr_list = sorted(attr_list)

        header = args.entity_type + "_id\t" + "\t".join(attr_list)
        print_(header)

        for entity_dict in entities:
            name = entity_dict['name']
            etype = entity_dict['entityType']
            attrs = entity_dict['attributes']
            line = name
            for attr in attr_list:
                ##Handle values that aren't just strings
                if attr == 'participant':
                    p = attrs.get(attr, None)
                    pname = p['entityName'] if p is not None else ""
                    line += "\t" + pname
                elif attr == 'samples':
                    slist = attrs.get(attr, [])
                    snames = ",".join([s['entityName'] for s in slist])
                    line += "\t" + snames
                else:
                    line += "\t" + str(attrs.get(attr, ""))
            print_(line)

    #Otherwise get workspace attributes
    else:
        r = fapi.get_workspace(args.project, args.workspace, args.api_url)
        fapi._check_response_code(r, 200)

        workspace_attrs = r.json()['workspace']['attributes']

        for k in sorted(workspace_attrs.keys()):
            if k in args.attributes or len(args.attributes) == 0:
                print_(k + "\t" + workspace_attrs[k])


@fiss_cmd
def attr_fill_null(args):
    """
    Assign the null sentinel value for all entities which do not have a value
    for the given attributes.

    see gs://broad-institute-gdac/GDAC_FC_NULL for more details
    """
    NULL_SENTINEL = "gs://broad-institute-gdac/GDAC_FC_NULL"
    attrs = args.attributes

    if not attrs:
        print_("Error: provide at least one attribute to set")
        return 1

    if 'participant' in attrs or 'samples' in attrs:
        print_("Error: can't assign null to samples or participant")
        return 1

    # Set entity attributes
    if args.entity_type is not None:
        print_("Collecting entity data...")
        # Get existing attributes
        entities = _entity_paginator(args.project, args.workspace,
                                     args.entity_type,
                                     page_size=1000, filter_terms=None,
                                     sort_direction="asc",api_root=args.api_url)

        # samples need participant_id as well
        #TODO: This may need more fixing for other types
        orig_attrs = list(attrs)
        if args.entity_type == "sample":
            attrs.insert(0, "participant_id")

        header = "entity:" + args.entity_type + "_id\t" + "\t".join(attrs)
        # Book keep the number of updates for each attribute
        attr_update_counts = {a : 0 for a in orig_attrs}

        # construct new entity data by inserting null sentinel, and counting
        # the number of updates
        entity_data = []
        for entity_dict in entities:
            name = entity_dict['name']
            etype = entity_dict['entityType']
            e_attrs = entity_dict['attributes']
            line = name
            altered = False
            for attr in attrs:
                if attr == "participant_id":
                    line += "\t" + e_attrs['participant']['entityName']
                    continue # This attribute is never updated by fill_null
                if attr not in e_attrs:
                    altered = True
                    attr_update_counts[attr] += 1
                line += "\t" + str(e_attrs.get(attr, NULL_SENTINEL))
            # Improve performance by only updating records that have changed
            if altered:
                entity_data.append(line)

        # Check to see if all entities are being set to null for any attributes
        # This is usually a mistake, so warn the user
        num_entities = len(entities)
        prompt = "Continue? [Y\\n]: "
        for attr in orig_attrs:
            if num_entities == attr_update_counts[attr]:
                message = "WARNING: no {0}s with attribute '{1}'\n".format(
                    args.entity_type, attr
                )
                if not args.yes and not _confirm_prompt(message, prompt):
                    #Don't do it!
                    return

        # check to see if no sentinels are necessary
        if not any(c != 0 for c in itervalues(attr_update_counts)):
            print_("No null sentinels required, exiting...")
            return

        if args.to_loadfile:
            print_("Saving loadfile to " + args.to_loadfile)
            with open(args.to_loadfile, "w") as f:
                f.write(header + '\n')
                f.write("\n".join(entity_data))
            return

        updates_table = "     count attribute\n"
        for attr in sorted(attr_update_counts):
            count = attr_update_counts[attr]
            updates_table += "{0:>10} {1}\n".format(count, attr)

        message = "WARNING: This will insert null sentinels for "
        message += "these attributes:\n" + updates_table
        if not args.yes and not _confirm_prompt(message):
            #Don't do it!
            return

        # Chunk the entities into batches of 500, and upload to FC
        print_("Batching " + str(len(entity_data)) + " updates to Firecloud...")
        chunk_len = 500
        total = int(len(entity_data) / chunk_len) + 1
        batch = 0
        for i in range(0, len(entity_data), chunk_len):
            batch += 1
            print_("Updating samples {0}-{1}, batch {2}/{3}".format(
                i+1, min(i+chunk_len, len(entity_data)), batch, total
            ))
            this_data = header + '\n' + '\n'.join(entity_data[i:i+chunk_len])

            # Now push the entity data back to firecloud
            r = fapi.upload_entities(args.project, args.workspace, this_data,
                                     args.api_url)
            fapi._check_response_code(r, 200)

        print_("Done.")
    else:
        # TODO: set workspace attributes
        print_("attr_fill_null requires an entity type")
        return 1


@fiss_cmd
def ping(args):
    """ Ping FireCloud Server """
    r = fapi.ping(args.api_url)
    fapi._check_response_code(r, 200)
    print_(r.content)


@fiss_cmd
def mop(args):
    """ Clean up unreferenced data in a workspace """
    # First retrieve the workspace to get the bucket information
    if args.verbose:
        print_("Retrieving workspace information...")
    r = fapi.get_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)
    workspace = r.json()
    bucket = workspace['workspace']['bucketName']
    bucket_prefix = 'gs://' + bucket
    workspace_name = workspace['workspace']['name']

    if args.verbose:
        print_("{0} -- {1}".format(workspace_name, bucket_prefix))

    referenced_files = set()
    for value in workspace['workspace']['attributes'].values():
        if isinstance(value, string_types) and value.startswith(bucket_prefix):
            referenced_files.add(value)


    # TODO: Make this more efficient with a native api call?
    # # Now run a gsutil ls to list files present in the bucket
    try:
        gsutil_args = ['gsutil', 'ls', 'gs://' + bucket + '/**']
        if args.verbose:
            print_(' '.join(gsutil_args))
        bucket_files = subprocess.check_output(gsutil_args, stderr=subprocess.PIPE)
        # Check output produces a string in Py2, Bytes in Py3, so decode if necessary
        if type(bucket_files) == bytes:
            bucket_files = bucket_files.decode()

    except subprocess.CalledProcessError as e:
        print_("Error retrieving files from bucket: " + e)
        return 1

    bucket_files = set(bucket_files.strip().split('\n'))
    if args.verbose:
        num = len(bucket_files)
        print_("Found {0} files in bucket {1}".format(num, bucket))

    # Now build a set of files that are referenced in the bucket
    # 1. Get a list of the entity types in the workspace
    r = fapi.list_entity_types(args.project, args.workspace,
                              args.api_url)
    fapi._check_response_code(r, 200)
    entity_types = r.json().keys()

    # 2. For each entity type, request all the entities
    for etype in entity_types:
        if args.verbose:
            print_("Getting annotations for " + etype + " entities...")
        # use the paginated version of the query
        entities = _entity_paginator(args.project, args.workspace, etype,
                              page_size=1000, filter_terms=None,
                              sort_direction="asc", api_root=fapi.PROD_API_ROOT)

        for entity in entities:
            for value in entity['attributes'].values():
                if isinstance(value, string_types) and value.startswith(bucket_prefix):
                    # 'value' is a file in this bucket
                    referenced_files.add(value)

    if args.verbose:
        num = len(referenced_files)
        print_("Found {0} referenced files in workspace {1}".format(num, workspace_name))

    # Set difference shows files in bucket that aren't referenced
    unreferenced_files = bucket_files - referenced_files

    # Filter out files like .logs and rc.txt
    def can_delete(f):
        '''Return true if this file should not be deleted in a mop.'''
        # Don't delete logs
        if f.endswith('.log'):
            return False
        # Don't delete return codes from jobs
        if f.endswith('-rc.txt'):
            return False
        # Don't delete tool's exec.sh
        if f.endswith('exec.sh'):
            return False

        return True

    deleteable_files = [f for f in unreferenced_files if can_delete(f)]

    if len(deleteable_files) == 0:
        print_("No files to mop in " + workspace['workspace']['name'])
        return

    if args.verbose or args.dry_run:
        print_("Found {0} files to delete:\n".format(len(deleteable_files))
               + "\n".join(deleteable_files ) + '\n')

    message = "WARNING: Delete {0} files in {1} ({2})".format(
        len(deleteable_files), bucket_prefix, workspace['workspace']['name'])
    if args.dry_run or (not args.yes and not _confirm_prompt(message)):
        #Don't do it!
        return

    # Pipe the deleteable_files into gsutil rm to remove them
    gsrm_args = ['gsutil', '-m', 'rm', '-I']
    PIPE = subprocess.PIPE
    STDOUT=subprocess.STDOUT
    if args.verbose:
        print_("Deleting files with gsutil...")
    gsrm_proc = subprocess.Popen(gsrm_args, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    # Pipe the deleteable_files into gsutil
    result = gsrm_proc.communicate(input='\n'.join(deleteable_files))[0]
    if args.verbose:
        print_(result.rstrip())


@fiss_cmd
def flow_submit(args):
    """Submit a workflow on the given entity"""
    print_("Submitting {0} on {1} in {2}/{3}".format(
        args.config, args.entity, args.project, args.workspace
    ))
    r = fapi.create_submission(args.project, args.workspace,
                               args.namespace, args.config,
                               args.entity, args.entity_type, args.expression,
                               args.api_url)
    fapi._check_response_code(r, 201)
    # Give submission id in response
    sub_id = r.json()['submissionId']
    print_("Submission successful. Submission_id: " + sub_id )


@fiss_cmd
def sset_loop(args):
    """ Loop over sample sets in a workspace, performing a func """
    # Ensure the action is a valid fiss_cmd
    fiss_func = __cmd_to_func(args.action)
    if not fiss_func:
        eprint("ERROR: invalid FISS cmd '" + args.action + "'")
        return 1

    #First get the sample sets
    r = fapi.get_entities(args.project, args.workspace,
                          "sample_set", args.api_url)
    fapi._check_response_code(r, 200)

    sample_sets = [entity['name'] for entity in r.json()]

    # Ensure entity-type is sample_set
    args.entity_type = "sample_set"

    for sset in sample_sets:
        print_('\n' + args.action + " " + sset + ":")

        args.entity = sset

        # Call the fiss_func, and stop on errors, unless using -k option
        try:
            code = fiss_func(args)
        except Exception as e:
            eprint("Error: " + str(e))
            code = -1

        if not args.keep_going and code:
            return code


@fiss_cmd
def monitor(args):
    """ View submitted jobs in a workspace. """
    r = fapi.list_submissions(args.project, args.workspace, args.api_url)
    print_(r.content)
    print_(len(r.json()))

#################################################
# Utilities
#################################################

def _confirm_prompt(message, prompt="\nAre you sure? [Y\\n]: ",
                    affirmations=("Y", "Yes", "yes", "y")):
    """
    Display a message, then confirmation prompt, and return true
    if the user responds with one of the affirmations.
    """
    answer = input(message + prompt)
    return answer in affirmations

def _nonempty_project(string):
    """
    Argparse validator for ensuring a workspace is provided
    """
    value = str(string)
    if len(value) == 0:
        msg = "No project provided and no DEFAULT_PROJECT found"
        raise argparse.ArgumentTypeError(msg)
    return value

def _entity_paginator(namespace, workspace, etype, page_size=100,
                      filter_terms=None, sort_direction="asc",
                      api_root=fapi.PROD_API_ROOT):
    """Pages through the get_entities_query endpoint to get all entities in
       the workspace without crashing.
    """

    page = 1
    all_entities = []
    # Make initial request
    r = fapi.get_entities_query(namespace, workspace, etype, page=page,
                           page_size=page_size, sort_direction=sort_direction,
                           filter_terms=filter_terms, api_root=api_root)
    fapi._check_response_code(r, 200)

    response_body = r.json()
    # Get the total number of pages
    total_pages = response_body['resultMetadata']['filteredPageCount']

    # append the first set of results
    entities = response_body['results']
    all_entities.extend(entities)
    # Now iterate over remaining pages to retrieve all the results
    page = 2
    while page <= total_pages:
        r = fapi.get_entities_query(namespace, workspace, etype, page=page,
                               page_size=page_size, sort_direction=sort_direction,
                               filter_terms=filter_terms, api_root=api_root)
        fapi._check_response_code(r, 200)
        entities = r.json()['results']
        all_entities.extend(entities)
        page += 1

    return all_entities

def eprint(*args, **kwargs):
    """ Print a message to stderr """
    print_(*args, file=sys.stderr, **kwargs)


def __cmd_to_func(cmd):
    """ Returns the function object in this module matching cmd. """
    fiss_module = sys.modules[__name__]
    # Returns None if string is not a recognized FISS command
    func = getattr(fiss_module, cmd, None)
    if func and not hasattr(func, 'fiss_cmd'):
        func = None
    return func


#################################################
# Main, entrypoint for fissfc
################################################


def main():
    #Set defaults using CLI default values
    default_api_url = fapi.PROD_API_ROOT
    default_project = ''

    # Load any plugins, in case we need to override defaults
    manager = PluginManager()
    manager.setPluginPlaces(PLUGIN_PLACES)
    manager.collectPlugins()

    # Using the plugins, load defaults
    for pluginInfo in manager.getAllPlugins():
        #API_URL
        default_api_url = getattr(pluginInfo.plugin_object,
                                  'API_URL',
                                   default_api_url)
        # Default Google project
        default_project = getattr(pluginInfo.plugin_object,
                                  'DEFAULT_PROJECT',
                                  default_project)

    default_project_list = [default_project] if default_project != '' else []

    #Initialize core parser
    #TODO: Add longer description
    u  = 'fissfc [OPTIONS] CMD [arg ...]\n'
    u += '       fissfc [ --help | -v | --version ]'
    parser = argparse.ArgumentParser(usage=u,
                                     description='FISS: The FireCloud CLI')

    # Core Flags
    url_help = 'Fireclould api url. Your default is ' + default_api_url
    parser.add_argument('-u', '--url', dest='api_url',
                        default=default_api_url,
                        help=url_help)

    parser.add_argument("-v", "--version",
                        action='version', version=__version__)

    parser.add_argument('-V', '--verbose',
                            action='store_true', help='Increase verbosity')

    parser.add_argument("-y", "--yes", action='store_true',
                            help="Assume yes for any prompts")

    # Many commands share arguments, and we can make parent parsers to make it
    # easier to reuse arguments. Commands that operate on workspaces
    # all take a google project and a workspace name

    workspace_parent = argparse.ArgumentParser(add_help=False)
    workspace_parent.add_argument('-w', '--workspace',
                                  required=True, help='Workspace name')

    # project is required if there is no default_project
    proj_required = not bool(default_project)
    proj_help =  'Google Project (workspace namespace). Required '
    proj_help += 'if no DEFAULT_PROJECT is stored in a plugin'
    workspace_parent.add_argument('-p', '--project', default=default_project,
                                  help=proj_help, required=proj_required)

    # Commands that update ACL roles require a role and list of users
    acl_parent = argparse.ArgumentParser(add_help=False)
    acl_parent.add_argument('-r', '--role', help='ACL role', required=True,
                           choices=['OWNER', 'READER', 'WRITER', 'NO ACCESS'])
    acl_parent.add_argument('--users', help='FireCloud usernames', nargs='+',
                            required=True)

    # Commands that operates on entity_types
    etype_parent = argparse.ArgumentParser(add_help=False)
    etype_parent.add_argument('-t', '--entity-type', required=True,
                             help="FireCloud entity type")

    # Commands that require an entity name
    entity_parent = argparse.ArgumentParser(add_help=False)
    entity_parent.add_argument('-e', '--entity', required=True,
                             help="FireCloud entity name")

    # Commands that work with methods
    meth_parent = argparse.ArgumentParser(add_help=False)
    meth_parent.add_argument('-m', '--method', required=True,
                             help='Workflow/Method name')
    meth_parent.add_argument('-n', '--namespace', required=True,
                             help='Method namespace')

    # Commands that work with method configurations
    conf_parent = argparse.ArgumentParser(add_help=False)
    conf_parent.add_argument('-c', '--config', required=True,
                             help='Method configuration name')
    conf_parent.add_argument('-n', '--namespace', required=True,
                             help='Method configuration namespace')

    # Commands that need a snapshot_id
    snapshot_parent = argparse.ArgumentParser(add_help=False)
    snapshot_parent.add_argument('-i', '--snapshot-id', required=True,
                                 help="Snapshot ID (version) of method/config")

    # Commands that take an optional list of attributes
    attr_parent = argparse.ArgumentParser(add_help=False)
    attr_parent.add_argument('-a', '--attributes', nargs='*', metavar='attr',
                            help='List of attributes')

    ## Create one subparser for each fiss equivalent
    subparsers = parser.add_subparsers(help=argparse.SUPPRESS)

    # Create Workspace
    snew_parser = subparsers.add_parser('space_new', parents=[workspace_parent],
                                        description='Create new workspace')
    phelp = 'Create a protected (dbGaP-controlled) workspace.'
    phelp += 'You must have linked NIH credentials for this option.'
    snew_parser.add_argument('--protected', action='store_true', help=phelp)
    snew_parser.set_defaults(func=space_new)

    #Delete workspace
    spdel_parser = subparsers.add_parser(
        'space_delete', parents=[workspace_parent],
        description='Delete workspace'
    )
    spdel_parser.set_defaults(func=space_delete)

    # Get workspace information
    si_parser = subparsers.add_parser(
        'space_info', description='Show workspace information',
        parents=[workspace_parent]
    )
    si_parser.set_defaults(func=space_info)

    # List workspaces
    space_list_parser = subparsers.add_parser(
        'space_list', description='List available workspaces'
    )
    space_list_parser.set_defaults(func=space_list)

    #Lock workspace
    space_lock_parser = subparsers.add_parser(
        'space_lock', description='Lock a workspace',
        parents=[workspace_parent]
    )
    space_lock_parser.set_defaults(func=space_lock)

    # Unlock Workspace
    space_unlock_parser = subparsers.add_parser(
        'space_unlock', description='Unlock a workspace',
        parents=[workspace_parent]
    )
    space_unlock_parser.set_defaults(func=space_unlock)

    #Clone workspace
    clone_desc = 'Clone a workspace. The destination namespace or name must be '
    clone_desc += 'different from the workspace being cloned'
    clone_parser = subparsers.add_parser(
        'space_clone', description=clone_desc, parents=[workspace_parent]
    )
    clone_parser.add_argument("-P", "--to-project",
                               help="Project (Namespace) of clone workspace")
    clone_parser.add_argument("-W", "--to-workspace",
                               help="Name of clone workspace")
    clone_parser.set_defaults(func=space_clone)

    #Import data into a workspace
    import_parser = subparsers.add_parser(
        'entity_import', description='Import data into a workspace',
        parents=[workspace_parent]
    )
    import_parser.add_argument('-f','--tsvfile', help='Tab-delimited loadfile')
    import_parser.set_defaults(func=entity_import)

    #List of entity types in a workspace
    et_parser = subparsers.add_parser(
        'entity_types', parents=[workspace_parent],
        description='List entity types in a workspace'
    )
    et_parser.set_defaults(func=entity_types)

    #List of entities in a workspace
    el_parser = subparsers.add_parser(
        'entity_list', description='List entity types in a workspace',
        parents=[workspace_parent]
    )
    el_parser.set_defaults(func=entity_list)

    # List entities in tsv format
    # REMOVED: see entity_tsv()
    # etsv_parser = subparsers.add_parser(
    #     'entity_tsv', description='Get a tsv of workspace entities',
    #     parents=[workspace_parent, etype_parent]
    # )
    # etsv_parser.set_defaults(func=entity_tsv)

    #List of participants
    pl_parser = subparsers.add_parser(
        'participant_list', description='List participants in a workspace',
        parents=[workspace_parent]
    )
    pl_parser.set_defaults(func=participant_list)

    #List of samples
    sl_parser = subparsers.add_parser(
        'sample_list', description='List samples in a workspace',
        parents=[workspace_parent]
    )
    sl_parser.set_defaults(func=sample_list)

    #List of sample sets
    ssetl_parser = subparsers.add_parser(
        'sset_list', description='List sample sets in a workspace',
        parents=[workspace_parent]
    )
    ssetl_parser.set_defaults(func=sset_list)

    #Delete entity in a workspace
    edel_parser = subparsers.add_parser(
        'entity_delete', description='Delete entity in a workspace',
        parents=[workspace_parent, etype_parent, entity_parent]
    )
    edel_parser.set_defaults(func=entity_delete)

    partdel_parser = subparsers.add_parser(
        'participant_delete', description='Delete participant in a workspace',
        parents=[workspace_parent, entity_parent]
    )
    partdel_parser.set_defaults(func=participant_delete)

    sdel_parser = subparsers.add_parser(
        'sample_delete', description='Delete sample in a workspace',
        parents=[workspace_parent, entity_parent]
    )
    sdel_parser.set_defaults(func=sample_delete)

    ssdel_parser = subparsers.add_parser(
        'sset_delete', description='Delete sample set in a workspace',
        parents=[workspace_parent, entity_parent]
    )
    ssdel_parser.set_defaults(func=sset_delete)

    #Show workspace roles
    wacl_parser = subparsers.add_parser(
        'space_acl', description='Show users and roles in workspace',
        parents=[workspace_parent]
    )
    wacl_parser.set_defaults(func=space_acl)

    #Set workspace roles
    sacl_prsr = subparsers.add_parser(
        'space_set_acl', description='Show users and roles in workspace',
        parents=[workspace_parent, acl_parent]
    )
    sacl_prsr.set_defaults(func=space_set_acl)

    #Push a new workflow to the methods repo
    nf_parser = subparsers.add_parser(
        'flow_new', description='Push workflow to the methods repository',
        parents=[meth_parent]
    )
    wdl_help = 'Workflow Description Language (WDL) file'
    nf_parser.add_argument('-d','--wdl', help=wdl_help, required=True)
    syn_help = 'Short (<80 chars) description of method'
    nf_parser.add_argument('-s', '--synopsis', help=syn_help)
    nf_parser.add_argument('--doc', help='Optional documentation file <10Kb')
    nf_parser.set_defaults(func=flow_new)

    #Redact a method
    mredact_parser=subparsers.add_parser(
        'flow_delete', description='Redact method from the methods repository',
        parents=[meth_parent, snapshot_parent]
    )
    mredact_parser.set_defaults(func=flow_delete)

    #Method acl operations:
    #Get ACL
    methacl_parser = subparsers.add_parser(
        'flow_acl', description='Show users and roles for a method',
        parents=[meth_parent, snapshot_parent]
    )
    methacl_parser.set_defaults(func=flow_acl)

    #Set ACL
    macl_parser = subparsers.add_parser(
        'flow_set_acl', description='Show users and roles in workspace',
        parents=[meth_parent, snapshot_parent, acl_parent]
    )
    macl_parser.set_defaults(func=flow_set_acl)

    # List available methods
    flow_list_parser = subparsers.add_parser(
        'flow_list', description='List available workflows'
    )
    flow_list_parser.set_defaults(func=flow_list)

    # List available configurations
    cfg_list_parser = subparsers.add_parser(
        'config_list', description='List available configurations'
    )
    cfg_list_parser.add_argument('-w', '--workspace', help='Workspace name')
    proj_help =  'Google Project (workspace namespace).'
    cfg_list_parser.add_argument('-p', '--project', default=default_project,
                                 help=proj_help)
    cfg_list_parser.set_defaults(func=config_list)

    #Config ACLs
    cfgacl_parser = subparsers.add_parser(
        'config_acl', description='Show users and roles for a configuration',
        parents=[conf_parent, snapshot_parent]
    )
    cfgacl_parser.set_defaults(func=config_acl)

    #Set ACL
    # cacl_parser = subparsers.add_parser('config_set_acl',
    #                           description='Set roles for config')
    # cacl_parser.add_argument('namespace', help='Method namespace')
    # cacl_parser.add_argument('name', help='Config name')
    # cacl_parser.add_argument('snapshot_id', help='Snapshot ID')
    # cacl_parser.add_argument('role', help='ACL role',
    #                      choices=['OWNER', 'READER', 'WRITER', 'NO ACCESS'])
    # cacl_parser.add_argument('users', metavar='user', help='Firecloud username',
    #                          nargs='+')
    # cacl_parser.set_defaults(func=flow_set_acl)

    # Status
    status_prsr = subparsers.add_parser(
        'ping', description='Show status of FireCloud services')
    status_prsr.set_defaults(func=ping)

    #Get attribute values on workspace or entities
    attr_parser = subparsers.add_parser(
        'attr_get', description='Get attributes from entities in a workspace',
        parents=[workspace_parent, attr_parent]
    )
    # Duplicate entity-type here, because it is optional for attr_get
    etype_help =  'Entity type to retrieve annotations from. '
    etype_help += 'If omitted, workspace annotations will be retrieved'
    attr_parser.add_argument(
        '-t', '--entity-type', help=etype_help,
        choices=[
            'participant', 'participant_set', 'sample', 'sample_set',
            'pair', 'pair_set'
        ]
    )
    attr_parser.set_defaults(func=attr_get)

    # Set null sentinel values
    attrf_parser = subparsers.add_parser(
        'attr_fill_null', description='Assign NULL sentinel value to attributes',
        parents=[workspace_parent, etype_parent, attr_parent]
    )
    attrf_parser.add_argument("-o", "--to-loadfile", metavar='loadfile',
                              help="Save changes to provided loadfile, but do not perform update")
    attrf_parser.set_defaults(func=attr_fill_null)

    # Delete unreferenced files from a workspace's bucket
    mop_parser = subparsers.add_parser(
        'mop', description='Remove unused files from a workspace\'s bucket',
        parents=[workspace_parent]
    )
    mop_parser.add_argument('--dry-run', action='store_true',
                            help='Show deletions that would be performed')
    mop_parser.set_defaults(func=mop)

    # Submit a workflow
    flow_submit_prsr = subparsers.add_parser(
        'flow_submit', description='Submit a workflow in a workspace',
        parents=[workspace_parent, conf_parent, entity_parent]
    )
    #Duplicate entity type here since we want sample_set to be default
    etype_help =  'Entity type to assign null values, if attribute is missing.'
    etype_help += '\nDefault: sample_set'
    flow_submit_prsr.add_argument(
        '-t', '--entity-type', help=etype_help,
        default='sample_set',
        choices=[
            'participant', 'participant_set', 'sample', 'sample_set',
            'pair', 'pair_set'
        ]
    )
    expr_help = "(optional) Entity expression to use when entity type doesn't"
    expr_help += " match the method configuration. Example: 'this.samples'"
    flow_submit_prsr.add_argument('-x', '--expression', help=expr_help)
    flow_submit_prsr.set_defaults(func=flow_submit)

    # Loop over sample sets, performing a command
    ssloop_help = 'Loop over sample sets in a workspace, performing <action>'
    ssloop_parser = subparsers.add_parser(
        'sset_loop', description=ssloop_help,
        parents=[workspace_parent, attr_parent]
    )
    ssloop_parser.add_argument('action', help='FISS command to execute')
    ssloop_parser.add_argument('-c', '--config',
                               help='Method configuration name')
    ssloop_parser.add_argument('-n', '--namespace',
                               help='Method configuration namespace')
    khelp = "Loop through all sample sets, ignoring errors"
    ssloop_parser.add_argument('-k', '--keep-going', action='store_true',
                               help=khelp)
    ssloop_parser.add_argument('-x', '--expression', help=expr_help)
    ssloop_parser.set_defaults(func=sset_loop)

    mon_parser = subparsers.add_parser('monitor', help="Monitor submitted jobs.",
        parents=[workspace_parent]
    )
    mon_parser.set_defaults(func=monitor)

    # Add any commands from the plugin
    for pluginInfo in manager.getAllPlugins():
        pluginInfo.plugin_object.register_commands(subparsers)

    ##Special cases, print help with no arguments
    if len(sys.argv) == 1:
            parser.print_help()
    elif sys.argv[1]=='-l':
        #Print commands in a more readable way
        choices=[]
        for a in parser._actions:
            if isinstance(a, argparse._SubParsersAction):
                for choice, _ in a.choices.items():
                    choices.append(choice)

        # next arg is search term, if specified
        search = ''
        if len(sys.argv) > 2:
            search = sys.argv[2]
        for c in sorted(choices):
            if search in c:
                print_('\t' + c)
    elif sys.argv[1] == '-F':
        ## Show source for remaining args
        for fname in sys.argv[2:]:
            # Get module name
            fiss_module = sys.modules[__name__]
            try:
                func = getattr(fiss_module, fname)
                source_lines = ''.join(getsourcelines(func)[0])
                print_(source_lines)
            except AttributeError:
                pass
    else:
        ##Otherwise parse args and call correct subcommand
        args = parser.parse_args()

        sys.exit(args.func(args))


if __name__ == '__main__':
    main()
