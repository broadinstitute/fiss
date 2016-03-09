#! /usr/bin/env python

from fissfc import firecloud_api as fapi
from argparse import ArgumentParser, _SubParsersAction, ArgumentTypeError
import json
import sys
from six import print_
from yapsy.PluginManager import PluginManager
import os


__version__="0.5.0"
PLUGIN_PLACES = [os.path.expanduser('~/.fissfc/plugins'), "plugins"]

#################################################
# SubCommands
#################################################

def space_list(args):
    response, content = fapi.list_workspaces(args.api_url)
    _err_response(response, content, [200])

    #Parse the JSON for the workspace + namespace
    workspaces = json.loads(content)
    results = []
    for space in workspaces:
        ns = space['workspace']['namespace']
        ws = space['workspace']['name']
        if ns in args.namespaces or len(args.namespaces) == 0:
            results.append(ns + '\t' + ws)

    #Sort for easier viewing, ignore case
    results = sorted(results, key=lambda s: s.lower())
    for r in results:
        print_(r)

def space_lock(args):
    response, content = fapi.lock_workspace(args.namespace,
                                            args.workspace, args.api_url)
    _err_response(response, content, [204])
    print_('Locked workspace {0}/{1}'.format(args.namespace, args.workspace))

def space_new(args):
    response, content = fapi.create_workspace(args.namespace, args.workspace,
                                              dict(), args.api_url)
    _err_response(response, content, [201])
    print_('Created workspace {0}/{1}'.format(args.namespace, args.workspace))

def space_info(args):
    r, c = fapi.get_workspace(args.namespace, args.workspace, args.api_url)
    _err_response(r, c, [200])

    #TODO?: pretty_print_workspace(c)
    print_(c)

def space_delete(args):
    if not args.yes and not _are_you_sure("Delete workspace: {0}/{1}".format(args.namespace,
                                                        args.workspace)):
        #Don't do it!
        return 

    response, content = fapi.delete_workspace(args.namespace, 
                                                 args.workspace,
                                                 args.api_url)
    _err_response(response, content, [202])
    print_('Deleted workspace {0}/{1}'.format(args.namespace, args.workspace))

def space_unlock(args):
    response, content = fapi.unlock_workspace(args.namespace,
                                            args.workspace, args.api_url)
    _err_response(response, content, [204])
    print_('Unlocked workspace {0}/{1}'.format(args.namespace, args.workspace))

def space_clone(args):
    response, content = fapi.clone_workspace(args.from_namespace,
                                             args.from_workspace,
                                             args.to_namespace,
                                             args.to_workspace,
                                             args.api_url)
    _err_response(response, content, [201])
    print_('Successfully cloned workspace')

def entity_import(args):
    response, content = fapi.upload_entities(args.namespace, args.workspace,
                                            args.file, args.api_url)
    _err_response(response, content, [200, 201])
    print_('Successfully uploaded entities')

def entity_types(args):
    r, c = fapi.workspace_entity_types(args.namespace, args.workspace, 
                                       args.api_url)
    _err_response(r,c, [200])
    for etype in json.loads(c):
        print_(etype)

def entity_list(args):
    r, c = fapi.get_workspace_entities_with_type(args.namespace,
                                                args.workspace, 
                                                args.api_url)
    _err_response(r,c, [200])
    for entity in json.loads(c):
        print_('{0}\t{1}'.format(entity['entityType'], entity['name']))

def entity_list_tsv(args):
    r, c = fapi.get_workspace_entities_tsv(args.namespace,
                                                args.workspace, 
                                                args.etype,
                                                args.api_url)
    _err_response(r,c, [200])
    print_(c)

def participant_list(args):
    r, c = fapi.get_workspace_entities(args.namespace, args.workspace,
                                       "participant", args.api_url)
    _err_response(r,c, [200])
    for entity in json.loads(c):
        print_(entity['name'])

def sample_list(args):
    r, c = fapi.get_workspace_entities(args.namespace, args.workspace,
                                       "sample", args.api_url)
    _err_response(r,c, [200])
    for entity in json.loads(c):
        print_(entity['name'])

def sample_set_list(args):
    r, c = fapi.get_workspace_entities(args.namespace, args.workspace,
                                       "sample_set", args.api_url)
    _err_response(r,c, [200])
    for entity in json.loads(c):
        print_(entity['name'])

def entity_delete(args):
    if not args.yes and not _are_you_sure("Delete " + args.type + " " + args.name + 
                         "in " + args.namespace + "/" + args.workspace):
        #Don't do it!
        return 

    r, c = fapi.delete_entity(args.namespace, args.workspace,
                              args.type, args.name, args.api_url)
    _err_response(r,c, [204])
    print_("Succesfully deleted " + args.type)

def participant_delete(args):
    args.type = "participant"
    return entity_delete(args)

def sample_delete(args):
    args.type = "sample"
    return entity_delete(args)

def sample_set_delete(args):
    args.type = "sample_set"
    return entity_delete(args)

def space_acl(args):
    r, c = fapi.get_workspace_acl(args.namespace, args.workspace, args.api_url)
    _err_response(r, c, [200])
    for user, role in json.loads(c).iteritems():
        print_('{0}\t{1}'.format(user, role))

def space_set_acl(args):
    acl_updates = [{"email": user, "accessLevel": args.role} for user in args.users]
    r, c = fapi.update_workspace_acl(args.namespace, args.workspace, 
                                    acl_updates, args.api_url)
    _err_response(r, c, [200])
    print_("Successfully updated roles")

def flow_new(args):
    r, c = fapi.push_workflow(args.namespace, args.name, args.synopsis,
                              args.wdl, args.doc, args.api_url)
    _err_response(r, c, [201])
    print_("Successfully pushed workflow")

def flow_delete(args):
    if not args.yes and not _are_you_sure("Delete workflow " + args.namespace +
                                      "/" + args.name + ":" + args.snapshot_id):
        #Don't do it!
        return 
    r, c = fapi.redact_workflow(args.namespace, args.name, 
                                args.snapshot_id, args.api_url)
    _err_response(r,c, [200])
    print_("Successfully redacted workflow")

def flow_acl(args):
    r, c = fapi.get_repository_method_acl(args.namespace, 
                                args.name, args.snapshot_id, args.api_url)
    _err_response(r,c, [200])
    for d in json.loads(c):
        user = d['user']
        role = d['role']
        print_('{0}\t{1}'.format(user, role))

def flow_set_acl(args):
    acl_updates = [{"user": user, "role": args.role} for user in args.users]
    r, c = fapi.update_repository_method_acl(args.namespace, args.name, 
                                            args.snapshot_id, acl_updates,
                                            args.api_url)
    _err_response(r,c, [200])
    print_("Successfully set method acl")

def flow_list(args):
    response, content = fapi.get_repository_methods(args.api_url)
    _err_response(response, content, [200])

    #Parse the JSON for the workspace + namespace
    methods = json.loads(content)
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
    response, content = fapi.get_repository_configurations(args.api_url)
    _err_response(response, content, [200])

    #Parse the JSON for the workspace + namespace
    methods = json.loads(content)
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
    r, c = fapi.get_repository_configuration_acl(args.namespace, 
                                args.name, args.snapshot_id, args.api_url)
    _err_response(r,c, [200])
    for d in json.loads(c):
        user = d['user']
        role = d['role']
        print_('{0}\t{1}'.format(user, role))


def attr_get(args):
    ##if entities was specified
    if args.etype is not None:
        r, c = fapi.get_workspace_entities_with_type(args.namespace,
                                                      args.workspace,
                                                      args.api_url)
        _err_response(r,c, [200])

        dict_response = json.loads(c)

        #Filter entities to only the one asked for
        matching_entities = [d for d in dict_response if d['entityType'] == args.etype]

        all_attr = [d['attributes'] for d in matching_entities]

        #Union of all keys in the dictionary, i.e. all possible attributes
        attr_list = set().union(*all_attr) if args.attributes == 'all' else args.attributes.split(',')
        attr_list = sorted(attr_list)

        header = args.etype + "_id\t" + "\t".join(attr_list)
        print_(header)

        for entity_dict in matching_entities:
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
                    slist = attrs.get(attr, None)
                    snames = ",".join([s['entityName'] for s in slist]) if slist is not None else ""
                    line += "\t" + snames
                else:
                    line += "\t" + str(attrs.get(attr, ""))
            print_(line)

    #Otherwise get workspace attributes
    else:
        r, c = fapi.get_workspace(args.namespace, args.workspace, args.api_url)
        _err_response(r, c, [200])

        workspace_attrs = json.loads(c)['workspace']['attributes']

        for k in sorted(workspace_attrs.keys()):
            if k in args.attributes.split(',') or args.attributes=='all':
                print_(k + "\t" + workspace_attrs[k])


#################################################
# Utilities
#################################################

def _are_you_sure(action):
    """
    Prompts the user to agree (Y/y) to the proposed action.

    Returns true on (Y, Yes, y, yes), any other input is false
    """
    agreed = ("Y", "Yes", "yes", "y")
    answer = raw_input("WARNING: This will \n\t{0} \nAre you sure? [Y\\n]: ".format(action))
    return answer in agreed

def _err_response(response, content, expected):
    """
    Throws an exception if the response status is unexpected
    """
    if response.status not in expected:
        emsg = "Unexpected server response: {0}\n{1}".format(response.status,
                                                             content)
        raise RuntimeError(emsg)

def _nonempty_workspace(string):
    """
    Argparse validator for ensuring a workspace is provided
    """
    value = str(string)
    if len(value) == 0:
        msg = "No namespace provided and no DEFAULT_PROJECT found"
        raise ArgumentTypeError(msg)
    return value

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
        default_api_url = getattr(pluginInfo.plugin_object, 'API_URL', default_api_url)
        # Default Google project
        default_project = getattr(pluginInfo.plugin_object, 'DEFAULT_PROJECT', default_project)

    default_project_list = [default_project] if default_project != '' else []

    #Initialize core parser
    parser = ArgumentParser(description='The Firecloud CLI for fiss users')

    # Core Flags
    parser.add_argument('-u', '--url', dest='api_url',
                        default=default_api_url,
                        help='Firecloud api url. Your default is \
                             {0}'.format(default_api_url))

    parser.add_argument('-l', '--list',
                        action='store_true',
                        help='List available actions')

    parser.add_argument("-v", "--version", action='version', version=__version__)

    parser.add_argument("-y", "--yes", action='store_true', 
                            help="Assume yes for any prompts. \
                            Necessary for scripting delete commands")

    # One subparser for each fiss equivalent
    subparsers = parser.add_subparsers(help='Supported commands')


    #Delete workspace
    space_delete_parser = subparsers.add_parser('space_delete',
                                                description='Delete workspace')
    space_delete_parser.add_argument('namespace', help='Workspace namespace')
    space_delete_parser.add_argument('workspace', help='Workspace name')
    space_delete_parser.set_defaults(func=space_delete)

    # List workspaces
    space_list_parser = subparsers.add_parser('space_list',
                                    description='List available workspaces')
    space_list_parser.add_argument('namespaces', metavar='namespace',nargs='*',
                         help='Only return workspaces from these namespaces.' + 
                              'If none are specified, list only workspaces in ' +
                              'your DEFAULT_PROJECT, otherwise all workspaces',
                         default=default_project_list)
    space_list_parser.set_defaults(func=space_list)

    #Lock workspace
    space_lock_parser = subparsers.add_parser('space_lock',
                                              description='Lock a workspace')
    space_lock_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT',)
    space_lock_parser.add_argument('workspace', help='Workspace name')
    space_lock_parser.set_defaults(func=space_lock)

    # Create Workspace
    space_new_parser = subparsers.add_parser('space_new',
                                            description='Create new workspace')
    space_new_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    space_new_parser.add_argument('workspace', help='Workspace name')
    space_new_parser.set_defaults(func=space_new)

    # Get workspace information
    si_parser = subparsers.add_parser('space_info',
                                    description='Show workspace information')
    si_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    si_parser.add_argument('workspace', help='Workspace name')
    si_parser.set_defaults(func=space_info)

    # Unlock Workspace
    space_unlock_parser = subparsers.add_parser('space_unlock',
                                              description='Unlock a workspace')
    space_unlock_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
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
    import_parser = subparsers.add_parser('entity_import', 
                                description='Import data into a workspace')
    import_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    import_parser.add_argument('workspace', help='Workspace name')
    import_parser.add_argument('file', help='Tab-delimited loadfile')
    import_parser.set_defaults(func=entity_import)

    #List of entity types in a workspace
    et_parser = subparsers.add_parser('entity_types',
                              description='List entity types in a workspace')
    et_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    et_parser.add_argument('workspace', help='Workspace name')
    et_parser.set_defaults(func=entity_types)

    #List of entities in a workspace
    el_parser = subparsers.add_parser('entity_list',
                              description='List entitities in a workspace')
    el_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    el_parser.add_argument('workspace', help='Workspace name')
    el_parser.set_defaults(func=entity_list)

    etsv_parser = subparsers.add_parser('entity_tsv',
                              description='Get a tsv of workspace entities')
    etsv_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    etsv_parser.add_argument('workspace', help='Workspace name')
    etsv_parser.add_argument('etype', help='Entity type')
    etsv_parser.set_defaults(func=entity_list_tsv)

    #List of participants
    pl_parser = subparsers.add_parser('participant_list',
                              description='List participants in a workspace')
    pl_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    pl_parser.add_argument('workspace', help='Workspace name')
    pl_parser.set_defaults(func=participant_list)

    #List of samples
    sl_parser = subparsers.add_parser('sample_list',
                              description='List samples in a workspace')
    sl_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    sl_parser.add_argument('workspace', help='Workspace name')
    sl_parser.set_defaults(func=sample_list)

    #List of sample sets
    ssetl_parser = subparsers.add_parser('sset_list',
                              description='List samples in a workspace')
    ssetl_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    ssetl_parser.add_argument('workspace', help='Workspace name')
    ssetl_parser.set_defaults(func=sample_set_list)

    #Delete entity in a workspace
    edel_parser = subparsers.add_parser('entity_delete',
                              description='Delete entity in a workspace')
    edel_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    edel_parser.add_argument('workspace', help='Workspace name')
    edel_parser.add_argument('type', help='Entity type')
    edel_parser.add_argument('name', help='Entity name')
    edel_parser.set_defaults(func=entity_delete)

    partdel_parser = subparsers.add_parser('participant_delete',
                              description='Delete participant in a workspace')
    partdel_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    partdel_parser.add_argument('workspace', help='Workspace name')
    partdel_parser.add_argument('name', help='Participant name')
    partdel_parser.set_defaults(func=participant_delete)

    sdel_parser = subparsers.add_parser('sample_delete',
                              description='Delete sample in a workspace')
    sdel_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    sdel_parser.add_argument('workspace', help='Workspace name')
    sdel_parser.add_argument('name', help='Sample name')
    sdel_parser.set_defaults(func=sample_delete)

    ssdel_parser = subparsers.add_parser('sset_delete',
                              description='Delete sample in a workspace')
    ssdel_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    ssdel_parser.add_argument('workspace', help='Workspace name')
    ssdel_parser.add_argument('name', help='Sample set name')
    ssdel_parser.set_defaults(func=sample_set_delete)

    #Show workspace roles
    wacl_parser = subparsers.add_parser('space_acl',
                              description='Show users and roles in workspace')
    wacl_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    wacl_parser.add_argument('workspace', help='Workspace name')
    wacl_parser.set_defaults(func=space_acl)

    #Set workspace roles
    sacl_parser = subparsers.add_parser('space_set_acl',
                              description='Show users and roles in workspace')
    sacl_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    sacl_parser.add_argument('workspace', help='Workspace name')
    sacl_parser.add_argument('role', help='ACL role',
                         choices=['OWNER', 'READER', 'WRITER', 'NO ACCESS'])
    sacl_parser.add_argument('users', metavar='user', help='Firecloud username',
                             nargs='+')
    sacl_parser.set_defaults(func=space_set_acl)

    #Push a new workflow to the methods repo
    nf_parser = subparsers.add_parser('flow_new',
                        description='Push a new workflow to the methods repository')
    nf_parser.add_argument('namespace', help='Methods namespace')
    nf_parser.add_argument('name', help='Method name')
    nf_parser.add_argument('wdl', help='Workflow Description Language (WDL) file')
    nf_parser.add_argument('synopsis', help='Short (<80 chars) description of method')
    nf_parser.add_argument('-d', '--doc', help='Optional documentation file <10Kb')
    nf_parser.set_defaults(func=flow_new)

    #Redact a method
    mredact_parser=subparsers.add_parser('flow_delete',
                            description='Redact a method from the methods repository')
    mredact_parser.add_argument('namespace', help='Methods namespace')
    mredact_parser.add_argument('name', help='Method name')
    mredact_parser.add_argument('snapshot_id', help='Snapshot ID')
    mredact_parser.set_defaults(func=flow_delete)

    #Method acl operations:
    #Get ACL
    methacl_parser = subparsers.add_parser('flow_acl',
                                description='Show users and roles for a method')
    methacl_parser.add_argument('namespace', help='Methods namespace')
    methacl_parser.add_argument('name', help='Method name')
    methacl_parser.add_argument('snapshot_id', help='Snapshot ID')
    methacl_parser.set_defaults(func=flow_acl)

    #Set ACL
    macl_parser = subparsers.add_parser('flow_set_acl',
                              description='Show users and roles in workspace')
    macl_parser.add_argument('namespace', help='Method namespace')
    macl_parser.add_argument('name', help='Method name')
    macl_parser.add_argument('snapshot_id', help='Snapshot ID')
    macl_parser.add_argument('role', help='ACL role',
                         choices=['OWNER', 'READER', 'WRITER', 'NO ACCESS'])
    macl_parser.add_argument('users', metavar='user', help='Firecloud username',
                             nargs='+')
    macl_parser.set_defaults(func=flow_set_acl)

    # List available methods
    flow_list_parser = subparsers.add_parser('flow_list',
                                    description='List available workflows')
    flow_list_parser.add_argument('namespaces', metavar='namespace',nargs='*',
                         help='Only return methods from these namespaces')
    flow_list_parser.set_defaults(func=flow_list)

    # List available configurations
    cfg_list_parser = subparsers.add_parser('config_list',
                                    description='List available configurations')
    cfg_list_parser.add_argument('namespaces', metavar='namespace',nargs='*',
                         help='Only return methods from these namespaces')
    cfg_list_parser.set_defaults(func=config_list)

    #Config ACLs
    cfgacl_parser = subparsers.add_parser('config_acl',
                                description='Show users and roles for a configuration')
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

    #Get attributes

    ##TODO: This is really messy. Perhaps this function should get it's own 
    # custom validator and take all arguments as one positional list,
    # and have a custom usage statement

    attr_parser = subparsers.add_parser('attr_get', 
                                usage='fissfc attr_get [-t ETYPE] [-a Attribute[,Attribute,...]]' +
                                       ' [namespace] workspace',
                                    description='Get attributes from entities in a workspace')
    attr_parser.add_argument('namespace', nargs="?", default=default_project,
                                    type=_nonempty_workspace,
                                    help='Workspace namespace. If not specified, ' +
                                    'this will be your DEFAULT_PROJECT')
    attr_parser.add_argument('workspace', help='Workspace name')

    attr_parser.add_argument('-t', '--entity-type', dest='etype',
                            help='Entity type to retrieve annotations from.' +
                            'If omitted, workspace annotations will be retrieved')
    attr_parser.add_argument('-a', '--attributes', default='all', 
                             help='Comma-separated list of Attributes to' +
                             'retrieve. If not specified' +
                             ' all attributes will be returned')
    attr_parser.set_defaults(func=attr_get)

    # Add any commands from the plugin
    for pluginInfo in manager.getAllPlugins():
        pluginInfo.plugin_object.register_commands(subparsers)


    ##Special case, print help with no arguments
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
    else:
        ##Otherwise parse args and call correct subcommand
        args = parser.parse_args()
        args.func(args)


if __name__ == '__main__':
    main()
