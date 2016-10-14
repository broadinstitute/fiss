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
from argparse import ArgumentParser, _SubParsersAction, ArgumentTypeError
import subprocess

from six import print_, iteritems, string_types
from six.moves import input
from yapsy.PluginManager import PluginManager

from firecloud import api as fapi
from firecloud.errors import *
from firecloud.__about__ import __version__

PLUGIN_PLACES = [os.path.expanduser('~/.fiss/plugins'), "plugins"]

#################################################
# SubCommands
#################################################

def space_list(args):
    """ List available workspaces. """
    r = fapi.list_workspaces(args.api_url)
    fapi._check_response_code(r, 200)

    #Parse the JSON for the workspace + namespace
    workspaces = r.json()
    get_all = args.all or len(args.namespaces) == 0
    pretty_spaces = []
    for space in workspaces:
        ns = space['workspace']['namespace']
        ws = space['workspace']['name']
        if get_all or ns in args.namespaces:
            pretty_spaces.append(ns + '\t' + ws)

    #Sort for easier viewing, ignore case
    pretty_spaces = sorted(pretty_spaces, key=lambda s: s.lower())
    for s in pretty_spaces:
        print_(s)

def space_lock(args):
    """  Lock a workspace. """
    r = fapi.lock_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 204)
    print_('Locked workspace {0}/{1}'.format(args.project, args.workspace))

def space_new(args):
    """ Create a new workspace. """
    r = fapi.create_workspace(args.project, args.workspace,
                                 args.protected, dict(), args.api_url)
    fapi._check_response_code(r, 201)
    print_('Created workspace {0}/{1}:'.format(args.project, args.workspace))
    print_(r.content)

def space_info(args):
    """ Get metadata for a workspace. """
    r = fapi.get_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)

    #TODO?: pretty_print_workspace(c)
    print_(r.content)

def space_delete(args):
    """ Delete a workspace. """
    prompt = "Delete workspace: {0}/{1}".format(args.project,
                                                args.workspace)
    if not args.yes and not _are_you_sure(prompt):
        #Don't do it!
        return

    r = fapi.delete_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 202)
    print_('Deleted workspace {0}/{1}'.format(args.project, args.workspace))

def space_unlock(args):
    """ Unlock a workspace. """
    r = fapi.unlock_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 204)
    print_('Unlocked workspace {0}/{1}'.format(args.project, args.workspace))

def space_clone(args):
    """ Clone a workspace """
    r = fapi.clone_workspace(
        args.from_namespace, args.from_workspace,
        args.to_namespace, args.to_workspace, args.api_url
    )
    fapi._check_response_code(r, 201)
    msg =  args.from_namespace + '/' + args.from_workspace
    msg += " successfully cloned to " + args.to_namespace
    msg += "/" + args.to_namespace
    print_(msg)

def entity_import(args):
    """ Upload an entity loadfile. """
    r = fapi.upload_entities_tsv(args.project, args.workspace,
                                 args.tsvfile, args.api_url)
    fapi._check_response_code(r, [200, 201])
    print_('Successfully uploaded entities')

def entity_types(args):
    """ List entity types in a workspace. """
    r = fapi.list_entity_types(args.project, args.workspace,
                              args.api_url)
    fapi._check_response_code(r, 200)
    for etype in r.json():
        print_(etype)

def entity_list(args):
    """ List entities in a workspace. """
    r = fapi.get_entities_with_type(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)
    for entity in r.json():
        print_('{0}\t{1}'.format(entity['entityType'], entity['name']))

def entity_tsv(args):
    """ Get list of entities in TSV format. """
    r = fapi.get_entities_tsv(args.project, args.workspace,
                              args.etype, args.api_url)
    fapi._check_response_code(r, 200)
    print_(r.content)

def participant_list(args):
    """ List participants in a workspace. """
    r = fapi.get_entities(args.project, args.workspace,
                          "participant", args.api_url)
    fapi._check_response_code(r, 200)
    for entity in r.json():
        print_(entity['name'])

def sample_list(args):
    """ List samples in a workspace. """
    r = fapi.get_entities(args.project, args.workspace,
                             "sample", args.api_url)
    fapi._check_response_code(r, 200)
    for entity in json.loads(c):
        print_(entity['name'])

def sset_list(args):
    """ List sample sets in a workspace """
    r = fapi.get_entities(args.project, args.workspace,
                          "sample_set", args.api_url)
    fapi._check_response_code(r, 200)

    for entity in r.json():
        print_(entity['name'])

def entity_delete(args):
    """ Delete entity in a workspace. """
    prompt = "Delete {0} {1} in {2}/{3}".format(
        args.etype, args.ename, args.project, args.workspace
    )
    if not args.yes and not _are_you_sure(prompt):
        #Don't do it!
        return
    r = fapi.delete_entity(args.project, args.workspace,
                           args.etype, args.ename, args.api_url)
    fapi._check_response_code(r, 204)
    print_("Succesfully deleted " + args.type)

def participant_delete(args):
    args.type = "participant"
    return entity_delete(args)

def sample_delete(args):
    args.type = "sample"
    return entity_delete(args)

def sset_delete(args):
    args.type = "sample_set"
    return entity_delete(args)

def space_acl(args):
    """ Get Access Control List for a workspace."""
    r = fapi.get_workspace_acl(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)
    for user, role in r.json().iteritems():
        print_('{0}\t{1}'.format(user, role))

def space_set_acl(args):
    """ Assign an ACL role to list of users for a workspace """
    acl_updates = [{"email": user,
                   "accessLevel": args.role} for user in args.users]
    r = fapi.update_workspace_acl(args.project, args.workspace,
                                  acl_updates, args.api_url)
    fapi._check_response_code(r, 200)
    print_("Successfully updated {0} role(s)".format(len(acl_updates)))

def flow_new(args):
    """ Submit a new workflow to the methods repository. """
    r = fapi.update_repository_method(args.namespace, args.name, args.synopsis,
                                      args.wdl, args.doc, args.api_url)
    fapi._check_response_code(r, 201)
    print_("Successfully pushed {0}/{1}".format(args.namespace, args.name))

def flow_delete(args):
    """ Redact a workflow in the methods repository """
    prompt = "Delete workflow {0}/{1}:{2}".format(
        args.namespace, args.name, args.snapshot_id)
    if not args.yes and not _are_you_sure(prompt):
        #Don't do it!
        return
    r = fapi.delete_repository_method(args.namespace, args.name,
                                      args.snapshot_id, args.api_url)
    fapi._check_response_code(r, 200)
    print_("Successfully redacted workflow.")

def flow_acl(args):
    """ Get Access Control List for a workflow """
    r = fapi.get_repository_method_acl(args.namespace, args.name,
                                       args.snapshot_id, args.api_url)
    fapi._check_response_code(r, 200)
    for d in r.json():
        user = d['user']
        role = d['role']
        print_('{0}\t{1}'.format(user, role))

def flow_set_acl(args):
    """ Assign an ACL role to a list of users for a worklow. """
    acl_updates = [{"user": user, "role": args.role} for user in args.users]
    r = fapi.update_repository_method_acl(args.namespace, args.name,
                                          args.snapshot_id, acl_updates,
                                          args.api_url)
    fapi._check_response_code(r, 200)
    print_("Successfully set method acl")

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
         if ns in args.namespaces or len(args.namespaces) == 0:
             results.append('{0}\t{1}\t{2}'.format(ns,n,sn_id))

    #Sort for easier viewing, ignore case
    results = sorted(results, key=lambda s: s.lower())
    for r in results:
        print_(r)

def config_list(args):
    """ List configurations in the methods repository. """
    r = fapi.list_repository_configs(args.api_url)
    fapi._check_response_code(r, 200)

    #Parse the JSON for the workspace + namespace
    methods = r.json()
    results = []
    for m in methods:
         ns = m['namespace']
         n = m['name']
         sn_id = m['snapshotId']
         if ns in args.namespaces or len(args.namespaces) == 0:
             results.append('{0}\t{1}\t{2}'.format(ns,n,sn_id))

    #Sort for easier viewing, ignore case
    results = sorted(results, key=lambda s: s.lower())
    for r in results:
        print_(r)

def config_acl(args):
    """ Get Access Control List for a method configuration. """
    r = fapi.get_repository_config_acl(args.namespace, args.name,
                                       args.snapshot_id, args.api_url)
    fapi._check_response_code(r, 200)
    for d in r.json():
        user = d['user']
        role = d['role']
        print_('{0}\t{1}'.format(user, role))


def attr_get(args):
    """ Get attributes from entities or workspaces. """
    ##if entities was specified
    if args.etype is not None:
        entities = _entity_paginator(args.project, args.workspace, args.etype,
                                     page_size=1000, filter_terms=None,
                                     sort_direction="asc",api_root=args.api_url)

        attr_list = args.attributes
        if not attr_list:
            # Get a set of all available attributes, then sort them
            attr_list = {k for e in entities for k in e['attributes'].keys()}
            attr_list = sorted(attr_list)

        header = args.etype + "_id\t" + "\t".join(attr_list)
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
        sys.exit(1)

    if 'participant' in attrs or 'samples' in attrs:
        print_("Error: can't assign null to samples or participant")
        sys.exit(1)

    # Set entity attributes
    if args.etype is not None:
        # Get existing attributes
        entities = _entity_paginator(args.project, args.workspace, args.etype,
                                     page_size=1000, filter_terms=None,
                                     sort_direction="asc",api_root=args.api_url)

        entity_data = "entity:" + args.etype + "_id\t" + "\t".join(attrs) + '\n'

        # Construct new entity data, replacing any null values with the sentinel
        null_count = 0
        for entity_dict in entities:
            name = entity_dict['name']
            etype = entity_dict['entityType']
            e_attrs = entity_dict['attributes']
            line = name
            for attr in attrs:
                if attr not in e_attrs:
                    null_count += 1
                line += "\t" + str(e_attrs.get(attr, NULL_SENTINEL))
                    
            entity_data += line + '\n'

        # Now push the entity data back to firecloud
        r = fapi.upload_entities(args.project, args.workspace, entity_data,
                                 args.api_url)
        fapi._check_response_code(r, 200)
        print_("Set " + str(null_count) + " null sentinels")
    else:
        # TODO: set workspace attributes
        print_("attr_fill_null requires an entity type")
        sys.exit(1)


def ping(args):
    """ Ping FireCloud Server """
    r = fapi.ping(args.api_url)
    fapi._check_response_code(r, 200)
    print_(r.content)

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
        sys.exit(1)

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
        r = fapi.get_entities(args.project, args.workspace,
                                  etype, args.api_url)
        fapi._check_response_code(r, 200)
        for entity in r.json():
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

    prompt = "delete {0} files in {1} ({2})".format(
        len(deleteable_files), bucket_prefix, workspace['workspace']['name'])
    if args.dry_run or (not args.yes and not _are_you_sure(prompt)):
        #Don't do it!
        return

    # Pipe the deleteable_files into gsutil rm to remove them
    gsrm_args = ['gsutil', '-m', 'rm', '-I']
    PIPE = subprocess.PIPE
    STDOUT=subprocess.STDOUT
    if args.verbose:
        print_("Deleting files with gsutil...")
    gsrm_proc = subprocess.Popen(gsrm_args, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    result = gsrm_proc.communicate(input='\n'.join(deleteable_files))[0]
    if args.verbose:
        print_(result.rstrip())


#################################################
# Utilities
#################################################

def _are_you_sure(action):
    """
    Prompts the user to agree (Y/y) to the proposed action.

    Returns true on (Y, Yes, y, yes), any other input is false
    """
    agreed = ("Y", "Yes", "yes", "y")
    prompt = "WARNING: This will \n\t" + action + "\nAre you sure? [Y\\n]: "
    answer = input(prompt)
    return answer in agreed

def _nonempty_project(string):
    """
    Argparse validator for ensuring a workspace is provided
    """
    value = str(string)
    if len(value) == 0:
        msg = "No project provided and no DEFAULT_PROJECT found"
        raise ArgumentTypeError(msg)
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
    parser = ArgumentParser(description='The FireCloud CLI for fiss users')

    # Core Flags
    url_help = 'Fireclould api url. Your default is ' + default_api_url
    parser.add_argument('-u', '--url', dest='api_url',
                        default=default_api_url,
                        help=url_help)

    parser.add_argument('-l', '--list',
                        action='store_true',
                        help='List available actions')

    parser.add_argument('-F', dest='show_source',
                        action='store_true',
                        help='Show implementation of named function')

    parser.add_argument("-v", "--version",
                        action='version', version=__version__)

    parser.add_argument("-y", "--yes", action='store_true',
                            help="Assume yes for any prompts")

    proj_help =  'Google Project (workspace namespace). Required for certain'
    proj_help += 'command if no DEFAULT_PROJECT is stored in a plugin'
    parser.add_argument("-p", "--project",
                        default=default_project, help=proj_help)

    # One subparser for each fiss equivalent
    subparsers = parser.add_subparsers(help='Supported commands')


    #Delete workspace
    space_delete_parser = subparsers.add_parser('space_delete',
                                                description='Delete workspace')
    space_delete_parser.add_argument('workspace', help='Workspace name')
    space_delete_parser.set_defaults(func=space_delete)

    # List workspaces
    space_list_parser = subparsers.add_parser(
        'space_list', description='List available workspaces')
    all_help = 'Get all available namespaces'
    space_list_parser.add_argument('-a', '--all',
                                   action='store_true', help=all_help)
    slist_help =  'Only return workspaces from these namespaces.'
    slist_help += 'If none are specified, list only workspaces in '
    slist_help += 'your DEFAULT_PROJECT, otherwise all workspaces'

    space_list_parser.add_argument(
        'namespaces', metavar='namespace', nargs='*',
         help=slist_help, default=default_project_list
    )
    space_list_parser.set_defaults(func=space_list)

    #Lock workspace
    space_lock_parser = subparsers.add_parser('space_lock',
                                              description='Lock a workspace')

    space_lock_parser.add_argument('workspace', help='Workspace name')
    space_lock_parser.set_defaults(func=space_lock)

    # Create Workspace
    snew_parser = subparsers.add_parser('space_new',
                                        description='Create new workspace')
    phelp = 'Create a protected (dbGaP-controlled) workspace.'
    phelp += 'You must have linked NIH credentials for this option.'
    snew_parser.add_argument('-p', '--protected',
                             action='store_true', help=phelp)
    snew_parser.add_argument('workspace', help='Workspace name')
    snew_parser.set_defaults(func=space_new)

    # Get workspace information
    si_parser = subparsers.add_parser(
        'space_info', description='Show workspace information')
    si_parser.add_argument('workspace', help='Workspace name')
    si_parser.set_defaults(func=space_info)

    # Unlock Workspace
    space_unlock_parser = subparsers.add_parser(
        'space_unlock', description='Unlock a workspace')
    space_unlock_parser.add_argument('workspace', help='Workspace name')
    space_unlock_parser.set_defaults(func=space_unlock)

    #Clone workspace
    clone_parser = subparsers.add_parser('space_clone',
                                         description='Clone a workspace')
    clone_parser.add_argument('from_namespace')
    clone_parser.add_argument('from_workspace')
    clone_parser.add_argument('to_namespace')
    clone_parser.add_argument('to_workspace')
    clone_parser.set_defaults(func=space_clone)

    #Import data into a workspace
    import_parser = subparsers.add_parser(
        'entity_import', description='Import data into a workspace')
    import_parser.add_argument('workspace', help='Workspace name')
    import_parser.add_argument('tsvfile', help='Tab-delimited loadfile')
    import_parser.set_defaults(func=entity_import)

    #List of entity types in a workspace
    et_parser = subparsers.add_parser(
        'entity_types', description='List entity types in a workspace')
    et_parser.add_argument('workspace', help='Workspace name')
    et_parser.set_defaults(func=entity_types)

    #List of entities in a workspace
    el_parser = subparsers.add_parser(
        'entity_list', description='List entitities in a workspace')
    el_parser.add_argument('workspace', help='Workspace name')
    el_parser.set_defaults(func=entity_list)

    etsv_parser = subparsers.add_parser(
        'entity_tsv', description='Get a tsv of workspace entities')
    etsv_parser.add_argument('workspace', help='Workspace name')
    etsv_parser.add_argument('etype', help='Entity type')
    etsv_parser.set_defaults(func=entity_tsv)

    #List of participants
    pl_parser = subparsers.add_parser(
        'participant_list', description='List participants in a workspace')
    pl_parser.add_argument('workspace', help='Workspace name')
    pl_parser.set_defaults(func=participant_list)

    #List of samples
    sl_parser = subparsers.add_parser(
        'sample_list', description='List samples in a workspace')
    sl_parser.add_argument('workspace', help='Workspace name')
    sl_parser.set_defaults(func=sample_list)

    #List of sample sets
    ssetl_parser = subparsers.add_parser(
        'sset_list', description='List sample sets in a workspace')
    ssetl_parser.add_argument('workspace', help='Workspace name')
    ssetl_parser.set_defaults(func=sset_list)

    #Delete entity in a workspace
    edel_parser = subparsers.add_parser(
        'entity_delete', description='Delete entity in a workspace')
    edel_parser.add_argument('workspace', help='Workspace name')
    edel_parser.add_argument('etype', help='Entity type')
    edel_parser.add_argument('ename', help='Entity name')
    edel_parser.set_defaults(func=entity_delete)

    partdel_parser = subparsers.add_parser(
        'participant_delete', description='Delete participant in a workspace')
    partdel_parser.add_argument('workspace', help='Workspace name')
    partdel_parser.add_argument('name', help='Participant name')
    partdel_parser.set_defaults(func=participant_delete)

    sdel_parser = subparsers.add_parser(
        'sample_delete', description='Delete sample in a workspace')
    sdel_parser.add_argument('workspace', help='Workspace name')
    sdel_parser.add_argument('name', help='Sample name')
    sdel_parser.set_defaults(func=sample_delete)

    ssdel_parser = subparsers.add_parser(
        'sset_delete', description='Delete sample set in a workspace')
    ssdel_parser.add_argument('workspace', help='Workspace name')
    ssdel_parser.add_argument('name', help='Sample set name')
    ssdel_parser.set_defaults(func=sset_delete)

    #Show workspace roles
    wacl_parser = subparsers.add_parser(
        'space_acl', description='Show users and roles in workspace')
    wacl_parser.add_argument('workspace', help='Workspace name')
    wacl_parser.set_defaults(func=space_acl)

    #Set workspace roles
    sacl_prsr = subparsers.add_parser(
        'space_set_acl', description='Show users and roles in workspace')
    sacl_prsr.add_argument('workspace', help='Workspace name')
    sacl_prsr.add_argument('role', help='ACL role',
                           choices=['OWNER', 'READER', 'WRITER', 'NO ACCESS'])
    sacl_prsr.add_argument('users', metavar='user', help='FireCloud username',
                           nargs='+')
    sacl_prsr.set_defaults(func=space_set_acl)

    #Push a new workflow to the methods repo
    nf_parser = subparsers.add_parser(
        'flow_new', description='Push workflow to the methods repository')
    nf_parser.add_argument('namespace', help='Methods namespace')
    nf_parser.add_argument('name', help='Method name')
    wdl_help = 'Workflow Description Language (WDL) file'
    nf_parser.add_argument('wdl', help=wdl_help)
    syn_help = 'Short (<80 chars) description of method'
    nf_parser.add_argument('synopsis', help=syn_help)
    doc_help = 'Optional documentation file <10Kb'
    nf_parser.add_argument('-d', '--doc', help=doc_help)
    nf_parser.set_defaults(func=flow_new)

    #Redact a method
    mredact_parser=subparsers.add_parser(
        'flow_delete', description='Redact method from the methods repository')
    mredact_parser.add_argument('namespace', help='Methods namespace')
    mredact_parser.add_argument('name', help='Method name')
    mredact_parser.add_argument('snapshot_id', help='Snapshot ID')
    mredact_parser.set_defaults(func=flow_delete)

    #Method acl operations:
    #Get ACL
    methacl_parser = subparsers.add_parser(
        'flow_acl', description='Show users and roles for a method')
    methacl_parser.add_argument('namespace', help='Methods namespace')
    methacl_parser.add_argument('name', help='Method name')
    methacl_parser.add_argument('snapshot_id', help='Snapshot ID')
    methacl_parser.set_defaults(func=flow_acl)

    #Set ACL
    macl_parser = subparsers.add_parser(
        'flow_set_acl', description='Show users and roles in workspace')
    macl_parser.add_argument('namespace', help='Method namespace')
    macl_parser.add_argument('name', help='Method name')
    macl_parser.add_argument('snapshot_id', help='Snapshot ID')
    macl_parser.add_argument(
        'role', help='ACL role',
        choices=['OWNER', 'READER', 'WRITER', 'NO ACCESS']
    )
    macl_parser.add_argument('users', metavar='user',
                             help='FireCloud username', nargs='+')
    macl_parser.set_defaults(func=flow_set_acl)

    # List available methods
    flow_list_parser = subparsers.add_parser(
        'flow_list', description='List available workflows')
    flow_list_parser.add_argument(
        'namespaces', metavar='namespace', nargs='*',
        help='Only return methods from these namespaces'
    )
    flow_list_parser.set_defaults(func=flow_list)

    # List available configurations
    cfg_list_parser = subparsers.add_parser(
        'config_list', description='List available configurations')
    cfg_list_parser.add_argument(
        'namespaces', metavar='namespace',nargs='*',
        help='Only return methods from these namespaces'
    )
    cfg_list_parser.set_defaults(func=config_list)

    #Config ACLs
    cfgacl_parser = subparsers.add_parser(
        'config_acl', description='Show users and roles for a configuration')
    cfgacl_parser.add_argument('namespace', help='Methods namespace')
    cfgacl_parser.add_argument('name', help='Config name')
    cfgacl_parser.add_argument('snapshot_id', help='Snapshot ID')
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


    #Get attributes
    attr_parser = subparsers.add_parser(
        'attr_get', description='Get attributes from entities in a workspace')
    attr_parser.add_argument('workspace', help='Workspace name')

    etype_help =  'Entity type to retrieve annotations from. '
    etype_help += 'If omitted, workspace annotations will be retrieved'
    attr_parser.add_argument(
        '-t', '--entity-type', dest='etype', help=etype_help,
        choices=['participant', 'participant_set',
                 'sample', 'sample_set',
                 'pair', 'pair_set'
                 ]
    )
    attr_help='Attributes to retrieve. If not specified all will be retrieved'
    attr_parser.add_argument('attributes', nargs='*', metavar='attribute',
                             help=attr_help)
    attr_parser.set_defaults(func=attr_get)

    # Set null sentinel values
    attrf_parser = subparsers.add_parser(
        'attr_fill_null', description='Assign NULL sentinel value to attributes')
    attrf_parser.add_argument('workspace', help='Workspace name')

    etype_help =  'Entity type to assign null values, if attribute is missing'
    attrf_parser.add_argument(
        '-t', '--entity-type', dest='etype', help=etype_help,
        choices=['participant', 'participant_set',
                 'sample', 'sample_set',
                 'pair', 'pair_set'
                 ]
    )

    attrf_help='Attributes to fill with null'
    attrf_parser.add_argument('attributes', nargs='*', metavar='attribute',
                             help=attr_help)
    attrf_parser.set_defaults(func=attr_fill_null)


    mop_parser = subparsers.add_parser(
        'mop', description='Remove unused files from a workspace\'s bucket'
    )
    mop_parser.add_argument('workspace', help='Workspace name')
    mop_parser.add_argument('--dry-run', action='store_true',
                            help='Show deletions that would be performed')
    mop_parser.add_argument('-y', '--yes', action='store_true',
                            help='Disable confirmation prompts')
    mop_parser.add_argument('-V', '--verbose',
                            action='store_true', help='Show actions')
    mop_parser.set_defaults(func=mop)

    # Add any commands from the plugin
    for pluginInfo in manager.getAllPlugins():
        pluginInfo.plugin_object.register_commands(subparsers)


    ##Special cases, print help with no arguments
    if len(sys.argv) == 1:
            parser.print_help()
            sys.exit(0)
    elif sys.argv[1]=='-l':
        #Print commands in a more readable way
        choices=[]
        for a in parser._actions:
            if isinstance(a, _SubParsersAction):
                for choice, _ in a.choices.items():
                    choices.append(choice)
        for c in sorted(choices):
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

        args.func(args)


if __name__ == '__main__':
    main()
