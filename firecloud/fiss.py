#! /usr/bin/env python
"""
FISS -- (Fi)reCloud (S)ervice (Selector)

This module provides a command line interface to FireCloud
For more details see https://software.broadinstitute.org/firecloud/
"""
from __future__ import print_function
import json
import sys
import os
import time
from inspect import getsourcelines
from traceback import print_tb as print_traceback
from io import open
import argparse
import subprocess
import re
import collections
from difflib import unified_diff
from six import iteritems, string_types, itervalues, u, text_type
from six.moves import input
from firecloud import api as fapi
from firecloud import fccore
from firecloud.errors import *
from firecloud.__about__ import __version__
from firecloud import supervisor

fcconfig = fccore.config_parse()

def fiss_cmd(function):
    """ Decorator to indicate a function is a FISS command """
    function.fiss_cmd = True
    return function

#################################################
# SubCommands
#################################################
@fiss_cmd
def space_list(args):
    ''' List accessible workspaces, in TSV form: <namespace><TAB>workspace'''

    r = fapi.list_workspaces()
    fapi._check_response_code(r, 200)

    spaces = []
    project = args.project
    if project:
        project = re.compile('^' + project)

    for space in r.json():
        ns = space['workspace']['namespace']
        if project and not project.match(ns):
            continue
        ws = space['workspace']['name']
        spaces.append(ns + '\t' + ws)

    # Sort for easier downstream viewing, ignoring case
    return sorted(spaces, key=lambda s: s.lower())

@fiss_cmd
def space_exists(args):
    """ Determine if the named space exists in the given project (namespace)"""
    # The return value is the INVERSE of UNIX exit status semantics, (where
    # 0 = good/true, 1 = bad/false), so to check existence in UNIX one would do
    #   if ! fissfc space_exists blah ; then
    #      ...
    #   fi
    try:
        r = fapi.get_workspace(args.project, args.workspace)
        fapi._check_response_code(r, 200)
        exists = True
    except FireCloudServerError as e:
        if e.code == 404:
            exists = False
        else:
            raise
    if fcconfig.verbosity:
        result = "DOES NOT" if not exists else "DOES"
        eprint('Space <%s> %s exist in project <%s>' %
                                (args.workspace, result, args.project))
    return exists

@fiss_cmd
def space_lock(args):
    """  Lock a workspace """
    r = fapi.lock_workspace(args.project, args.workspace)
    fapi._check_response_code(r, 204)
    if fcconfig.verbosity:
        eprint('Locked workspace {0}/{1}'.format(args.project, args.workspace))
    return 0

@fiss_cmd
def space_unlock(args):
    """ Unlock a workspace """
    r = fapi.unlock_workspace(args.project, args.workspace)
    fapi._check_response_code(r, 204)
    if fcconfig.verbosity:
        eprint('Unlocked workspace {0}/{1}'.format(args.project,args.workspace))
    return 0

@fiss_cmd
def space_new(args):
    """ Create a new workspace. """
    r = fapi.create_workspace(args.project, args.workspace,
                                 args.authdomain, dict())
    fapi._check_response_code(r, 201)
    if fcconfig.verbosity:
        eprint(r.content)
    return 0

@fiss_cmd
def space_info(args):
    """ Get metadata for a workspace. """
    r = fapi.get_workspace(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    return r.text

@fiss_cmd
def space_delete(args):
    """ Delete a workspace. """
    message = "WARNING: this will delete workspace: \n\t{0}/{1}".format(
        args.project, args.workspace)
    if not args.yes and not _confirm_prompt(message):
        return 0

    r = fapi.delete_workspace(args.project, args.workspace)
    fapi._check_response_code(r, [200, 202, 204, 404])
    if fcconfig.verbosity:
        print('Deleted workspace {0}/{1}'.format(args.project, args.workspace))
    return 0

@fiss_cmd
def space_clone(args):
    """ Replicate a workspace """
    # FIXME: add --deep copy option (shallow by default)
    #        add aliasing capability, then make space_copy alias
    if not args.to_workspace:
        args.to_workspace = args.workspace
    if not args.to_project:
        args.to_project = args.project
    if (args.project == args.to_project
        and args.workspace == args.to_workspace):
        eprint("Error: destination project and namespace must differ from"
               " cloned workspace")
        return 1

    r = fapi.clone_workspace(args.project, args.workspace, args.to_project,
                                                            args.to_workspace)
    fapi._check_response_code(r, 201)

    if fcconfig.verbosity:
        msg = "{}/{} successfully cloned to {}/{}".format(
                                            args.project, args.workspace,
                                            args.to_project, args.to_workspace)
        print(msg)

    return 0

@fiss_cmd
def space_acl(args):
    ''' Retrieve access control list for a workspace'''
    r = fapi.get_workspace_acl(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    result = dict()
    for user, info in sorted(r.json()['acl'].items()):
        result[user] = info['accessLevel']
    return result

@fiss_cmd
def space_set_acl(args):
    """ Assign an ACL role to list of users for a workspace """
    acl_updates = [{"email": user,
                   "accessLevel": args.role} for user in args.users]
    r = fapi.update_workspace_acl(args.project, args.workspace, acl_updates)
    fapi._check_response_code(r, 200)
    errors = r.json()['usersNotFound']

    if len(errors):
        eprint("Unable to assign role for unrecognized users:")
        for user in errors:
            eprint("\t{0}".format(user['email']))
        return 1

    if fcconfig.verbosity:
        print("Successfully updated {0} role(s)".format(len(acl_updates)))

    return 0

@fiss_cmd
def space_search(args):
    """ Search for workspaces matching certain criteria """
    r = fapi.list_workspaces()
    fapi._check_response_code(r, 200)

    # Parse the JSON for workspace + namespace; then filter by
    # search terms: each term is treated as a regular expression
    workspaces = r.json()
    extra_terms = []
    if args.bucket:
        workspaces = [w for w in workspaces
                      if re.search(args.bucket, w['workspace']['bucketName'])]
        extra_terms.append('bucket')

    # FIXME: add more filter terms
    pretty_spaces = []
    for space in workspaces:
        ns = space['workspace']['namespace']
        ws = space['workspace']['name']
        pspace = ns + '/' + ws
        # Always show workspace storage id
        pspace += '\t' + space['workspace']['bucketName']
        pretty_spaces.append(pspace)

    # Sort for easier viewing, ignore case
    return sorted(pretty_spaces, key=lambda s: s.lower())

@fiss_cmd
def entity_import(args):
    """ Upload an entity loadfile. """
    project = args.project
    workspace = args.workspace
    chunk_size = args.chunk_size
    model = args.model

    with open(args.tsvfile) as tsvf:
        headerline = tsvf.readline().strip()
        entity_data = [l.rstrip('\n') for l in tsvf]

    return _batch_load(project, workspace, headerline, entity_data, chunk_size,
                       model)

@fiss_cmd
def set_export(args):
    '''Return a list of lines in TSV form that would suffice to reconstitute a
       container (set) entity, if passed to entity_import.  The first line in
       the list is the header, and subsequent lines are the container members.
    '''

    r = fapi.get_entity(args.project, args.workspace, args.entity_type, args.entity)
    fapi._check_response_code(r, 200)
    set_type = args.entity_type
    set_name = args.entity
    member_type = set_type.split('_')[0]
    members = r.json()['attributes'][member_type+'s']['items']

    result = ["membership:{}_id\t{}_id".format(set_type, member_type)]
    result += ["%s\t%s" % (set_name, m['entityName']) for m in members ]
    return result

@fiss_cmd
def entity_types(args):
    """ List entity types in a workspace """
    r = fapi.list_entity_types(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    return r.json().keys()

@fiss_cmd
def entity_list(args):
    """ List entities in a workspace. """
    r = fapi.get_entities_with_type(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    return [ '{0}\t{1}'.format(e['entityType'], e['name']) for e in r.json() ]

# REMOVED: This now returns a *.zip* file containing two tsvs, which is far
# less useful for FISS users...
@fiss_cmd
def entity_tsv(args):
    """ Get list of entities in TSV format. Download files for which the
    encoding is undetected (e.g. ZIP archives). """
    r = fapi.get_entities_tsv(args.project, args.workspace,
                              args.entity_type, args.attrs, args.model)
    fapi._check_response_code(r, 200)
    if r.apparent_encoding is not None:
        return r.content.decode(r.apparent_encoding)
    else:
        content = r.headers['Content-Disposition'].split('; ')[-1].split('=')
        if len(content) == 2 and content[0] == 'filename':
            filename = content[1]
            if os.path.exists(filename) and (args.yes or not _confirm_prompt(
                                   'This will overwrite {}'.format(filename))):
                return
            with open(filename, 'wb') as outfile:
                for chunk in r:
                    outfile.write(chunk)
            print('Downloaded {}.'.format(filename))
            return
        else:
            eprint("Unable to determine name of file to download.")
            return 1

def __entity_names(entities):
    return [ entity['name'] for entity in entities ]

def __get_entities(args, kind, page_size=1000, handler=__entity_names):

    entities = _entity_paginator(args.project, args.workspace, kind,
                                                page_size=page_size)
    return handler(entities)

@fiss_cmd
def participant_list(args):
    ''' List participants within a container'''

    # Case 1: retrieve participants within a named data entity
    if args.entity_type and args.entity:
        # Edge case: caller asked for participant within participant (itself)
        if args.entity_type == 'participant':
            return [ args.entity.strip() ]
        # Otherwise retrieve the container entity
        r = fapi.get_entity(args.project, args.workspace, args.entity_type, args.entity)
        fapi._check_response_code(r, 200)
        participants = r.json()['attributes']["participants"]['items']
        return [ participant['entityName'] for participant in participants ]

    # Case 2: retrieve all participants within a workspace
    return __get_entities(args, "participant", page_size=2000)

@fiss_cmd
def pair_list(args):
    ''' List pairs within a container. '''

    # Case 1: retrieve pairs within a named data entity
    if args.entity_type and args.entity:
        # Edge case: caller asked for pair within a pair (itself)
        if args.entity_type == 'pair':
            return [ args.entity.strip() ]
        # Edge case: pairs for a participant, which has to be done hard way
        # by iteratiing over all samples (see firecloud/discussion/9648)
        elif args.entity_type == 'participant':
            entities = _entity_paginator(args.project, args.workspace,
                                     'pair', page_size=2000)
            return [ e['name'] for e in entities if
                     e['attributes']['participant']['entityName'] == args.entity]

        # Otherwise retrieve the container entity
        r = fapi.get_entity(args.project, args.workspace, args.entity_type, args.entity)
        fapi._check_response_code(r, 200)
        pairs = r.json()['attributes']["pairs"]['items']
        return [ pair['entityName'] for pair in pairs]

    # Case 2: retrieve all pairs within a workspace
    return __get_entities(args, "pair", page_size=2000)

@fiss_cmd
def sample_list(args):
    ''' List samples within a container. '''

    # Case 1: retrieve samples within a named data entity
    if args.entity_type and args.entity:
        # Edge case: caller asked for samples within a sample (itself)
        if args.entity_type == 'sample':
            return [ args.entity.strip() ]
        # Edge case: samples for a participant, which has to be done hard way
        # by iteratiing over all samples (see firecloud/discussion/9648)
        elif args.entity_type == 'participant':
            samples = _entity_paginator(args.project, args.workspace,
                                     'sample', page_size=2000)
            return [ e['name'] for e in samples if
                     e['attributes']['participant']['entityName'] == args.entity]

        # Otherwise retrieve the container entity
        r = fapi.get_entity(args.project, args.workspace, args.entity_type, args.entity)
        fapi._check_response_code(r, 200)
        if args.entity_type == 'pair':
            pair = r.json()['attributes']
            samples = [ pair['case_sample'], pair['control_sample'] ]
        else:
            samples = r.json()['attributes']["samples"]['items']

        return [ sample['entityName'] for sample in samples ]

    # Case 2: retrieve all samples within a workspace
    return __get_entities(args, "sample", page_size=2000)

@fiss_cmd
def sset_list(args):
    """ List sample sets in a workspace """
    return __get_entities(args, "sample_set")

@fiss_cmd
def entity_delete(args):
    """ Delete entity in a workspace. """

    msg = "WARNING: this will delete {0} {1} in {2}/{3}".format(
        args.entity_type, args.entity, args.project, args.workspace)

    if not (args.yes or _confirm_prompt(msg)):
        return

    json_body=[{"entityType": args.entity_type,
                "entityName": args.entity}]
    r = fapi.delete_entities(args.project, args.workspace, json_body)
    fapi._check_response_code(r, 204)
    if fcconfig.verbosity:
        print("Succesfully deleted " + args.type + " " + args.entity)

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
def meth_new(args):
    """ Submit a new workflow (or update) to the methods repository. """
    r = fapi.update_repository_method(args.namespace, args.method,
                                      args.synopsis, args.wdl, args.doc,
                                      args.comment)
    fapi._check_response_code(r, 201)
    if fcconfig.verbosity:
        print("Method %s installed to project %s" % (args.method,
                                                     args.namespace))
    return 0

@fiss_cmd
def meth_delete(args):
    """ Remove (redact) a method from the method repository """
    message = "WARNING: this will delete workflow \n\t{0}/{1}:{2}".format(
                                    args.namespace, args.method, args.snapshot_id)
    if not args.yes and not _confirm_prompt(message):
        return

    r = fapi.delete_repository_method(args.namespace, args.method,
                                                    args.snapshot_id)
    fapi._check_response_code(r, 200)
    if fcconfig.verbosity:
        print("Method %s removed from project %s" % (args.method,args.namespace))
    return 0

@fiss_cmd
def meth_wdl(args):
    ''' Retrieve WDL for given version of a repository method'''
    r = fapi.get_repository_method(args.namespace, args.method,
                                   args.snapshot_id, True)
    fapi._check_response_code(r, 200)
    return r.text

@fiss_cmd
def meth_acl(args):
    ''' Retrieve access control list for given version of a repository method'''
    r = fapi.get_repository_method_acl(args.namespace, args.method,
                                                    args.snapshot_id)
    fapi._check_response_code(r, 200)
    acls = sorted(r.json(), key=lambda k: k['user'])
    return map(lambda acl: '{0}\t{1}'.format(acl['user'], acl['role']), acls)

@fiss_cmd
def meth_set_acl(args):
    """ Assign an ACL role to a list of users for a workflow. """
    acl_updates = [{"user": user, "role": args.role} \
                   for user in set(expand_fc_groups(args.users)) \
                   if user != fapi.whoami()]

    id = args.snapshot_id
    if not id:
        # get the latest snapshot_id for this method from the methods repo
        r = fapi.list_repository_methods(namespace=args.namespace,
                                         name=args.method)
        fapi._check_response_code(r, 200)
        versions = r.json()
        if len(versions) == 0:
            if fcconfig.verbosity:
                eprint("method {0}/{1} not found".format(args.namespace,
                                                         args.method))
            return 1
        latest = sorted(versions, key=lambda m: m['snapshotId'])[-1]
        id = latest['snapshotId']

    r = fapi.update_repository_method_acl(args.namespace, args.method, id,
                                          acl_updates)
    fapi._check_response_code(r, 200)
    if fcconfig.verbosity:
        print("Updated ACL for {0}/{1}:{2}".format(args.namespace, args.method,
                                                   id))
    return 0

def expand_fc_groups(users, groups=None, seen=set()):
    """ If user is a firecloud group, return all members of the group.
    Caveat is that only group admins may do this.
    """
    for user in users:
        fcgroup = None
        if '@' not in user:
            fcgroup = user
        elif user.lower().endswith('@firecloud.org'):
            if groups is None:
                r = fapi.get_groups()
                fapi._check_response_code(r, 200)
                groups = {group['groupEmail'].lower():group['groupName'] \
                          for group in r.json() if group['role'] == 'Admin'}
            if user.lower() not in groups:
                if fcconfig.verbosity:
                    eprint("You do not have access to the members of {}".format(user))
                yield user
                continue
            else:
                fcgroup = groups[user.lower()]
        else:
            yield user
            continue
        
        # Avoid infinite loops due to nested groups
        if fcgroup in seen:
            continue
        
        r = fapi.get_group(fcgroup)
        fapi._check_response_code(r, [200, 403])
        if r.status_code == 403:
            if fcconfig.verbosity:
                eprint("You do not have access to the members of {}".format(fcgroup))
            continue
        fcgroup_data = r.json()
        seen.add(fcgroup)
        for admin in expand_fc_groups(fcgroup_data['adminsEmails'], groups, seen):
            yield admin
        for member in expand_fc_groups(fcgroup_data['membersEmails'], groups, seen):
            yield member
    

@fiss_cmd
def meth_list(args):
    """ List workflows in the methods repository """
    r = fapi.list_repository_methods(namespace=args.namespace,
                                     name=args.method,
                                     snapshotId=args.snapshot_id)
    fapi._check_response_code(r, 200)

    # Parse the JSON for the workspace + namespace
    methods = r.json()
    results = []
    for m in methods:
        ns = m['namespace']
        n = m['name']
        sn_id = m['snapshotId']
        results.append('{0}\t{1}\t{2}'.format(ns,n,sn_id))

    # Sort for easier viewing, ignore case
    return sorted(results, key=lambda s: s.lower())

@fiss_cmd
def meth_exists(args):
    '''Determine whether a given workflow is present in methods repo'''
    args.namespace = None
    args.snapshot_id = None
    return len(meth_list(args)) != 0

@fiss_cmd
def config_start(args):
    '''Invoke a task (method configuration), on given entity in given space'''

    # Try to use call caching (job avoidance)?  Flexibly accept range of answers
    cache = getattr(args, "cache", True)
    cache = cache is True or (cache.lower() in ["y", "true", "yes", "t", "1"])

    if not args.namespace:
        args.namespace = fcconfig.method_ns
    if not args.namespace:
        raise RuntimeError("namespace not provided, or configured by default")

    r = fapi.create_submission(args.project, args.workspace,args.namespace,
                            args.config, args.entity, args.entity_type,
                            args.expression, use_callcache=cache)
    fapi._check_response_code(r, 201)
    id = r.json()['submissionId']

    return ("Started {0}/{1} in {2}/{3}: id={4}".format(
        args.namespace, args.config, args.project, args.workspace, id)), id

@fiss_cmd
def config_stop(args):
    '''Abort a task (method configuration) by submission ID in given space'''

    r = fapi.abort_submission(args.project, args.workspace,
                              args.submission_id)
    fapi._check_response_code(r, 204)

    return ("Aborted {0} in {1}/{2}".format(args.submission_id,
                                            args.project,
                                            args.workspace))

@fiss_cmd
def config_list(args):
    """ List configuration(s) in the methods repository or a workspace. """
    verbose = fcconfig.verbosity
    if args.workspace:
        if verbose:
            print("Retrieving method configs from space {0}".format(args.workspace))
        if not args.project:
            eprint("No project given, and no default project configured")
            return 1
        r = fapi.list_workspace_configs(args.project, args.workspace)
        fapi._check_response_code(r, 200)
    else:
        if verbose:
            print("Retrieving method configs from method repository")
        r = fapi.list_repository_configs(namespace=args.namespace,
                                         name=args.config,
                                         snapshotId=args.snapshot_id)
        fapi._check_response_code(r, 200)

    # Parse the JSON for the workspace + namespace
    methods = r.json()
    results = []
    for m in methods:
        ns = m['namespace']
        if not ns:
            ns = '__EMPTYSTRING__'
        name = m['name']
        # Ugh: configs in workspaces look different from configs in methodRepo
        mver = m.get('methodRepoMethod', None)
        if mver:
            mver = mver.get('methodVersion', 'unknown')     # space config
        else:
            mver = m.get('snapshotId', 'unknown')           # repo  config
        results.append('{0}\t{1}\tsnapshotId:{2}'.format(ns, name, mver))

    # Sort for easier viewing, ignore case
    return sorted(results, key=lambda s: s.lower())

@fiss_cmd
def config_acl(args):
    ''' Retrieve access control list for a method configuration'''
    r = fapi.get_repository_config_acl(args.namespace, args.config,
                                                    args.snapshot_id)
    fapi._check_response_code(r, 200)
    acls = sorted(r.json(), key=lambda k: k['user'])
    return map(lambda acl: '{0}\t{1}'.format(acl['user'], acl['role']), acls)

@fiss_cmd
def config_set_acl(args):
    """ Assign an ACL role to a list of users for a config. """
    acl_updates = [{"user": user, "role": args.role} \
                   for user in set(expand_fc_groups(args.users)) \
                   if user != fapi.whoami()]

    id = args.snapshot_id
    if not id:
        # get the latest snapshot_id for this method from the methods repo
        r = fapi.list_repository_configs(namespace=args.namespace,
                                         name=args.config)
        fapi._check_response_code(r, 200)
        versions = r.json()
        if len(versions) == 0:
            if fcconfig.verbosity:
                eprint("Configuration {0}/{1} not found".format(args.namespace,
                                                                args.config))
            return 1
        latest = sorted(versions, key=lambda c: c['snapshotId'])[-1]
        id = latest['snapshotId']

    r = fapi.update_repository_config_acl(args.namespace, args.config, id,
                                          acl_updates)
    fapi._check_response_code(r, 200)
    if fcconfig.verbosity:
        print("Updated ACL for {0}/{1}:{2}".format(args.namespace, args.config,
                                                   id))
    return 0

@fiss_cmd
def config_get(args):
    """ Retrieve a method config from a workspace, send stdout """
    r = fapi.get_workspace_config(args.project, args.workspace,
                                        args.namespace, args.config)
    fapi._check_response_code(r, 200)
    # Setting ensure_ascii to False ensures unicode string returns
    return json.dumps(r.json(), indent=4, separators=(',', ': '),
                      sort_keys=True, ensure_ascii=False)

@fiss_cmd
def config_wdl(args):
    """ Retrieve the WDL for a method config in a workspace, send stdout """
    r = fapi.get_workspace_config(args.project, args.workspace,
                                  args.namespace, args.config)
    fapi._check_response_code(r, 200)
    
    method = r.json()["methodRepoMethod"]
    args.namespace   = method["methodNamespace"]
    args.method      = method["methodName"]
    args.snapshot_id = method["methodVersion"]
    
    return meth_wdl(args)

@fiss_cmd
def config_diff(args):
    """Compare method configuration definitions across workspaces. Ignores
       methodConfigVersion if the verbose argument is not set"""
    config_1 = config_get(args).splitlines()
    args.project = args.Project
    args.workspace = args.Workspace
    cfg_1_name = args.config
    if args.Config is not None:
        args.config = args.Config
    if args.Namespace is not None:
        args.namespace = args.Namespace
    config_2 = config_get(args).splitlines()
    if not args.verbose:
        config_1 = skip_cfg_ver(config_1)
        config_2 = skip_cfg_ver(config_2)
    return list(unified_diff(config_1, config_2, cfg_1_name, args.config, lineterm=''))

def skip_cfg_ver(cfg):
    return [line for line in cfg if not line.startswith('    "methodConfigVersion": ')]

@fiss_cmd
def config_put(args):
    '''Install a valid method configuration into a workspace, in one of several
       ways: from a JSON file containing a config definition (both file names
       and objects are supported); as a string representing the content of such
       a JSON file; or as a dict generated from such JSON content, e.g via
       json.loads(). Note that the CLI supports only string & filename input.'''

    config = args.config
    if os.path.isfile(config):
        with open(config, 'r') as fp:
            config = json.loads(fp.read())
    elif isinstance(config, str):
        config = json.loads(config)
    elif isinstance(config, dict):
        pass
    elif hasattr(config, "read"):
        config = json.loads(config.read())
    else:
        raise ValueError('Input method config must be filename, string or dict')

    r = fapi.create_workspace_config(args.project, args.workspace, config)
    fapi._check_response_code(r, [201])
    return True

__EDITME__ = u'EDITME, or abort edit/install by leaving entire config unchanged'
@fiss_cmd
def config_template(args):
    c = fapi.get_config_template(args.namespace, args.method, args.snapshot_id)
    fapi._check_response_code(c, 200)

    c = c.json()
    c[u'name'] = args.configname or __EDITME__
    c[u'namespace'] = args.namespace or __EDITME__
    c[u'rootEntityType'] = args.entity_type or __EDITME__
    outputs = c[u'outputs']
    for o in outputs:
        outputs[o] = __EDITME__
    inputs = c[u'inputs']
    for i in inputs:
        inputs[i] = __EDITME__

    return json.dumps(c, indent=4, separators=(',', ': '), sort_keys=True,
                      ensure_ascii=False)

@fiss_cmd
def config_edit(args):
    # Placeholder: accept either a method config name or a file containing
    # a method config definition (e.g. such as returned by config_get)
    pass

@fiss_cmd
def config_new(args):
    '''Attempt to install a new method config into a workspace, by: generating
       a template from a versioned method in the methods repo, then launching
       a local editor (respecting the $EDITOR environment variable) to fill in
       the incomplete input/output fields.  Returns True if the config was
       successfully installed, otherwise False'''

    cfg = config_template(args)
    # Iteratively try to edit/install the config: exit iteration by EITHER
    #   Successful config_put() after editing
    #   Leaving config unchanged in editor, e.g. quitting out of VI with :q
    #   FIXME: put an small integer upper bound on the # of loops here
    while True:
        try:
            edited = fccore.edit_text(cfg)
            if edited == cfg:
                eprint("No edits made, method config not installed ...")
                break
            if __EDITME__ in edited:
                eprint("Edit is incomplete, method config not installed ...")
                time.sleep(1)
                continue
            args.config = cfg = edited
            config_put(args)
            return True
        except FireCloudServerError as fce:
            __pretty_print_fc_exception(fce)

    return False

@fiss_cmd
def config_delete(args):
    """ Remove a method config from a workspace """
    r = fapi.delete_workspace_config(args.project, args.workspace,
                                        args.namespace, args.config)
    fapi._check_response_code(r, [200,204])
    return r.text if r.text else None

@fiss_cmd
def config_copy(args):
    """ Copy a method config to new name/space/namespace/project (or all 4) """
    if not (args.tospace or args.toname or args.toproject or args.tonamespace):
        raise RuntimeError('A new config name OR workspace OR project OR ' +
                           'namespace must be given (or all)')

    copy = fapi.get_workspace_config(args.fromproject, args.fromspace,
                                            args.namespace, args.config)
    fapi._check_response_code(copy, 200)

    copy = copy.json()
    if not args.toname:
        args.toname = args.config

    if not args.tonamespace:
        args.tonamespace = args.namespace

    if not args.toproject:
        args.toproject = args.fromproject

    if not args.tospace:
        args.tospace = args.fromspace

    copy['name'] = args.toname
    copy['namespace'] = args.tonamespace

    # Instantiate the copy
    r = fapi.overwrite_workspace_config(args.toproject, args.tospace,
                                        args.tonamespace, args.toname, copy)
    fapi._check_response_code(r, 200)

    if fcconfig.verbosity:
        print("Method %s/%s:%s copied to %s/%s:%s" % (
                args.fromproject, args.fromspace, args.config,
                args.toproject, args.tospace, args.toname))

    return 0

@fiss_cmd
def attr_get(args):
    '''Return a dict of attribute name/value pairs: if entity name & type
    are specified then attributes will be retrieved from that entity,
    otherwise workspace-level attributes will be returned.  By default all
    attributes attached to the given object will be returned, but a subset
    can be selected by specifying a list of attribute names; names which
    refer to a non-existent attribute will be silently ignored. By default
    a special __header__ entry is optionally added to the result. '''

    if args.entity_type and args.entity:
        r = fapi.get_entity(args.project, args.workspace, args.entity_type, args.entity)
        fapi._check_response_code(r, 200)
        attrs = r.json()['attributes']
        # It is wrong for the members of container objects to appear as metadata
        # (attributes) attached to the container, as it conflates fundamentally
        # different things: annotation vs membership. This can be seen by using
        # a suitcase model of reasoning: ATTRIBUTES are METADATA like the weight
        # & height of the suitcase, the shipping label and all of its passport
        # stamps; while MEMBERS are the actual CONTENTS INSIDE the suitcase.
        # This conflation also contradicts the design & docs (which make a clear
        # distinction between MEMBERSHIP and UPDATE loadfiles).  For this reason
        # a change has been requested of the FireCloud dev team (via forum), and
        # until it is implemented we will elide "list of members" attributes here
        # (but later may provide a way for users to request such, if wanted)
        for k in ["samples", "participants", "pairs"]:
            attrs.pop(k, None)
    else:
        r = fapi.get_workspace(args.project, args.workspace)
        fapi._check_response_code(r, 200)
        attrs = r.json()['workspace']['attributes']

    if args.attributes:         # return a subset of attributes, if requested
        attrs = {k:attrs[k] for k in set(attrs).intersection(args.attributes)}

    # If some attributes have been collected, return in appropriate format
    if attrs:
        if args.entity:                     # Entity attributes

            def textify(thing):
                if isinstance(thing, dict):
                    thing = thing.get("items", thing.get("entityName", "__UNKNOWN__"))
                return "{0}".format(thing)

            result = {args.entity : u'\t'.join(map(textify, attrs.values()))}
            # Add "hidden" header of attribute names, for downstream convenience
            object_id = u'entity:%s_id' % args.entity_type
            result['__header__'] = [object_id] + list(attrs.keys())
        else:
            result = attrs                  # Workspace attributes
    else:
        result = {}

    return result

@fiss_cmd
def attr_list(args):
    '''Retrieve names of all attributes attached to a given object, either
       an entity (if entity type+name is provided) or workspace (if not)'''
    args.attributes = None
    result = attr_get(args)
    names = result.get("__header__",[])
    if names:
        names = names[1:]
    else:
        names = result.keys()
    return sorted(names)

@fiss_cmd
def attr_set(args):
    ''' Set key=value attributes: if entity name & type are specified then
    attributes will be set upon that entity, otherwise the attribute will
    be set at the workspace level'''

    if args.entity_type and args.entity:
        prompt = "Set {0}={1} for {2}:{3} in {4}/{5}?\n[Y\\n]: ".format(
                            args.attribute, args.value, args.entity_type,
                            args.entity, args.project, args.workspace)

        if not (args.yes or _confirm_prompt("", prompt)):
            return 0

        update = fapi._attr_set(args.attribute, args.value)
        r = fapi.update_entity(args.project, args.workspace, args.entity_type,
                                                        args.entity, [update])
        fapi._check_response_code(r, 200)
    else:
        prompt = "Set {0}={1} in {2}/{3}?\n[Y\\n]: ".format(
            args.attribute, args.value, args.project, args.workspace
        )

        if not (args.yes or _confirm_prompt("", prompt)):
            return 0

        update = fapi._attr_set(args.attribute, args.value)
        r = fapi.update_workspace_attributes(args.project, args.workspace,
                                                                [update])
        fapi._check_response_code(r, 200)
    return 0

@fiss_cmd
def attr_delete(args):
    ''' Delete key=value attributes: if entity name & type are specified then
    attributes will be deleted from that entity, otherwise the attribute will
    be removed from the workspace'''

    if args.entity_type and args.entities:
        # Since there is no attribute deletion endpoint, we must perform 2 steps
        # here: first we retrieve the entity_ids, and any foreign keys (e.g.
        # participant_id for sample_id); and then construct a loadfile which
        # specifies which entities are to have what attributes removed.  Note
        # that FireCloud uses the magic keyword __DELETE__ to indicate that
        # an attribute should be deleted from an entity.

        # Step 1: see what entities are present, and filter to those requested
        entities = _entity_paginator(args.project, args.workspace,
                                     args.entity_type,
                                     page_size=1000, filter_terms=None,
                                     sort_direction="asc")
        if args.entities:
            entities = [e for e in entities if e['name'] in args.entities]

        # Step 2: construct a loadfile to delete these attributes
        attrs = sorted(args.attributes)
        etype = args.entity_type

        entity_data = []
        for entity_dict in entities:
            name = entity_dict['name']
            line = name
            # TODO: Fix other types?
            if etype == "sample":
                line += "\t" + entity_dict['attributes']['participant']['entityName']
            for attr in attrs:
                line += "\t__DELETE__"
            # Improve performance by only updating records that have changed
            entity_data.append(line)

        entity_header = ["entity:" + etype + "_id"]
        if etype == "sample":
            entity_header.append("participant_id")
        entity_header = '\t'.join(entity_header + list(attrs))

        # Remove attributes from an entity
        message = "WARNING: this will delete these attributes:\n\n" + \
                  ','.join(args.attributes) + "\n\n"
        if args.entities:
            message += 'on these {0}s:\n\n'.format(args.entity_type) + \
                       ', '.join(args.entities)
        else:
            message += 'on all {0}s'.format(args.entity_type)
        message += "\n\nin workspace {0}/{1}\n".format(args.project, args.workspace)
        if not args.yes and not _confirm_prompt(message):
            return 0

        # TODO: reconcile with other batch updates
        # Chunk the entities into batches of 500, and upload to FC
        if args.verbose:
            print("Batching " + str(len(entity_data)) + " updates to Firecloud...")
        chunk_len = 500
        total = int(len(entity_data) / chunk_len) + 1
        batch = 0
        for i in range(0, len(entity_data), chunk_len):
            batch += 1
            if args.verbose:
                print("Updating samples {0}-{1}, batch {2}/{3}".format(
                    i+1, min(i+chunk_len, len(entity_data)), batch, total
                ))
            this_data = entity_header + '\n' + '\n'.join(entity_data[i:i+chunk_len])

            # Now push the entity data back to firecloud
            r = fapi.upload_entities(args.project, args.workspace, this_data)
            fapi._check_response_code(r, 200)
    else:
        message = "WARNING: this will delete the following attributes in " + \
                  "{0}/{1}\n\t".format(args.project, args.workspace) + \
                  "\n\t".join(args.attributes)

        if not (args.yes or _confirm_prompt(message)):
            return 0

        updates = [fapi._attr_rem(a) for a in args.attributes]
        r = fapi.update_workspace_attributes(args.project, args.workspace,
                                             updates)
        fapi._check_response_code(r, 200)

    return 0

@fiss_cmd
def attr_copy(args):
    """ Copy workspace attributes between workspaces. """
    if not args.to_workspace:
        args.to_workspace = args.workspace
    if not args.to_project:
        args.to_project = args.project
    if (args.project == args.to_project
        and args.workspace == args.to_workspace):
        eprint("destination project and namespace must differ from"
               " source workspace")
        return 1

    # First get the workspace attributes of the source workspace
    r = fapi.get_workspace(args.project, args.workspace)
    fapi._check_response_code(r, 200)

    # Parse the attributes
    workspace_attrs = r.json()['workspace']['attributes']

    # If we passed attributes, only use those
    if args.attributes:
        workspace_attrs = {k:v for k, v in iteritems(workspace_attrs)
                           if k in args.attributes}

    if len(workspace_attrs) == 0:
        print("No workspace attributes defined in {0}/{1}".format(
            args.project, args.workspace))
        return 1

    message = "This will copy the following workspace attributes to {0}/{1}\n"
    message = message.format(args.to_project, args.to_workspace)
    for k, v in sorted(iteritems(workspace_attrs)):
        message += '\t{0}\t{1}\n'.format(k, v)

    if not args.yes and not _confirm_prompt(message):
        return 0

    # make the attributes into updates
    updates = [fapi._attr_set(k,v) for k,v in iteritems(workspace_attrs)]
    r = fapi.update_workspace_attributes(args.to_project, args.to_workspace,
                                                                    updates)
    fapi._check_response_code(r, 200)
    return 0

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
        print("Error: provide at least one attribute to set")
        return 1

    if 'participant' in attrs or 'samples' in attrs:
        print("Error: can't assign null to samples or participant")
        return 1

    # Set entity attributes
    if args.entity_type is not None:
        print("Collecting entity data...")
        # Get existing attributes
        entities = _entity_paginator(args.project, args.workspace,
                                     args.entity_type,
                                     page_size=1000, filter_terms=None,
                                     sort_direction="asc")

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
                    return

        # check to see if no sentinels are necessary
        if not any(c != 0 for c in itervalues(attr_update_counts)):
            print("No null sentinels required, exiting...")
            return 0

        if args.to_loadfile:
            print("Saving loadfile to " + args.to_loadfile)
            with open(args.to_loadfile, "w") as f:
                f.write(header + '\n')
                f.write("\n".join(entity_data))
            return 0

        updates_table = "     count attribute\n"
        for attr in sorted(attr_update_counts):
            count = attr_update_counts[attr]
            updates_table += "{0:>10} {1}\n".format(count, attr)

        message = "WARNING: This will insert null sentinels for " \
                  "these attributes:\n" + updates_table
        if not args.yes and not _confirm_prompt(message):
            return 0

        # Chunk the entities into batches of 500, and upload to FC
        print("Batching " + str(len(entity_data)) + " updates to Firecloud...")
        chunk_len = 500
        total = int(len(entity_data) / chunk_len) + 1
        batch = 0
        for i in range(0, len(entity_data), chunk_len):
            batch += 1
            print("Updating samples {0}-{1}, batch {2}/{3}".format(
                i+1, min(i+chunk_len, len(entity_data)), batch, total
            ))
            this_data = header + '\n' + '\n'.join(entity_data[i:i+chunk_len])

            # Now push the entity data back to firecloud
            r = fapi.upload_entities(args.project, args.workspace, this_data)
            fapi._check_response_code(r, 200)

        return 0
    else:
        # TODO: set workspace attributes
        print("attr_fill_null requires an entity type")
        return 1

@fiss_cmd
def health(args):
    """ Health FireCloud Server """
    r = fapi.health()
    fapi._check_response_code(r, 200)
    return r.content

@fiss_cmd
def mop(args):
    ''' Clean up unreferenced data in a workspace'''
    # First retrieve the workspace to get bucket information
    if args.verbose:
        print("Retrieving workspace information...")
    r = fapi.get_workspace(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    workspace = r.json()
    bucket = workspace['workspace']['bucketName']
    bucket_prefix = 'gs://' + bucket
    workspace_name = workspace['workspace']['name']

    if args.verbose:
        print("{0} -- {1}".format(workspace_name, bucket_prefix))

    referenced_files = set()
    for value in workspace['workspace']['attributes'].values():
        if isinstance(value, string_types) and value.startswith(bucket_prefix):
            referenced_files.add(value)

    # TODO: Make this more efficient with a native api call?
    # # Now run a gsutil ls to list files present in the bucket
    try:
        gsutil_args = ['gsutil', 'ls', 'gs://' + bucket + '/**']
        if args.verbose:
            print(' '.join(gsutil_args))
        bucket_files = subprocess.check_output(gsutil_args, stderr=subprocess.PIPE)
        # Check output produces a string in Py2, Bytes in Py3, so decode if necessary
        if type(bucket_files) == bytes:
            bucket_files = bucket_files.decode()

    except subprocess.CalledProcessError as e:
        eprint("Error retrieving files from bucket: " + str(e))
        return 1

    bucket_files = set(bucket_files.strip().split('\n'))
    if args.verbose:
        num = len(bucket_files)
        if args.verbose:
            print("Found {0} files in bucket {1}".format(num, bucket))

    # Now build a set of files that are referenced in the bucket
    # 1. Get a list of the entity types in the workspace
    r = fapi.list_entity_types(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    entity_types = r.json().keys()

    # 2. For each entity type, request all the entities
    for etype in entity_types:
        if args.verbose:
            print("Getting annotations for " + etype + " entities...")
        # use the paginated version of the query
        entities = _entity_paginator(args.project, args.workspace, etype,
                              page_size=1000, filter_terms=None,
                              sort_direction="asc")

        for entity in entities:
            for value in entity['attributes'].values():
                if isinstance(value, string_types) and value.startswith(bucket_prefix):
                    # 'value' is a file in this bucket
                    referenced_files.add(value)

    if args.verbose:
        num = len(referenced_files)
        print("Found {0} referenced files in workspace {1}".format(num, workspace_name))

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
        if args.verbose:
            print("No files to mop in " + workspace['workspace']['name'])
        return 0

    if args.verbose or args.dry_run:
        print("Found {0} files to delete:\n".format(len(deleteable_files))
               + "\n".join(deleteable_files ) + '\n')

    message = "WARNING: Delete {0} files in {1} ({2})".format(
        len(deleteable_files), bucket_prefix, workspace['workspace']['name'])
    if args.dry_run or (not args.yes and not _confirm_prompt(message)):
        return 0

    # Pipe the deleteable_files into gsutil rm to remove them
    gsrm_args = ['gsutil', '-m', 'rm', '-I']
    PIPE = subprocess.PIPE
    STDOUT=subprocess.STDOUT
    if args.verbose:
        print("Deleting files with gsutil...")
    gsrm_proc = subprocess.Popen(gsrm_args, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    # Pipe the deleteable_files into gsutil
    result = gsrm_proc.communicate(input='\n'.join(deleteable_files).encode())[0]
    if args.verbose:
        if type(result) == bytes:
            result = result.decode()
        print(result.rstrip())
    return 0

@fiss_cmd
def noop(args):
    if args.verbose:
        proj  = getattr(args, "project","unspecified")
        space = getattr(args, "workspace", "unspecified")
        print('fiss no-op command: Project=%s, Space=%s' % (proj, space))
    return 0

@fiss_cmd
def config_cmd(args):
    values = fccore.attrdict()
    names = args.variables
    if not names:
        names = list(fcconfig.keys())
    for name in names:
        values[name] = fcconfig.get(name, "__undefined__")
    return values

@fiss_cmd
def sset_loop(args):
    ''' Loop over all sample sets in a workspace, performing a func '''
    # Ensure that the requested action is a valid fiss_cmd
    fiss_func = __cmd_to_func(args.action)
    if not fiss_func:
        eprint("invalid FISS cmd '" + args.action + "'")
        return 1

    # First get the sample set names
    r = fapi.get_entities(args.project, args.workspace, "sample_set")
    fapi._check_response_code(r, 200)

    sample_sets = [entity['name'] for entity in r.json()]

    args.entity_type = "sample_set"
    for sset in sample_sets:
        print('\n# {0}::{1}/{2} {3}'.format(args.project, args.workspace, sset,
                                            args.action))
        args.entity = sset
        # Note how this code is similar to how args.func is called in
        # main so it may make sense to try to a common method for both
        try:
            result = fiss_func(args)
        except Exception as e:
            status = __pretty_print_fc_exception(e)
            if not args.keep_going:
                return status
        printToCLI(result)

    return 0

@fiss_cmd
def monitor(args):
    ''' Retrieve status of jobs submitted from a given workspace, as a list
        of TSV lines sorted by descending order of job submission date'''
    r = fapi.list_submissions(args.project, args.workspace)
    fapi._check_response_code(r, 200)
    statuses = sorted(r.json(), key=lambda k: k['submissionDate'], reverse=True)
    header = '\t'.join(list(statuses[0].keys()))
    expander = lambda v: '{0}'.format(v)

    def expander(thing):
        if isinstance(thing, dict):
            entityType = thing.get("entityType", None)
            if entityType:
                return "{0}:{1}".format(entityType, thing['entityName'])
        return "{0}".format(thing)

    # FIXME: this will generally return different column order between Python 2/3
    return [header] + ['\t'.join( map(expander, v.values())) for v in statuses]

@fiss_cmd
def supervise(args):
    ''' Run legacy, Firehose-style workflow of workflows'''
    project = args.project
    workspace = args.workspace
    namespace = args.namespace
    workflow = args.workflow
    sample_sets = args.sample_sets
    recovery_file = args.json_checkpoint

    # If no sample sets are provided, run on all sample sets
    if not sample_sets:
        r = fapi.get_entities(args.project, args.workspace, "sample_set")
        fapi._check_response_code(r, 200)
        sample_sets = [s['name'] for s in r.json()]

    message = "Sample Sets ({}):\n\t".format(len(sample_sets)) + \
              "\n\t".join(sample_sets)

    prompt = "\nLaunch workflow in " + project + "/" + workspace + \
             " on these sample sets? [Y\\n]: "

    if not args.yes and not _confirm_prompt(message, prompt):
        return

    return supervisor.supervise(project, workspace, namespace, workflow,
                                sample_sets, recovery_file)

@fiss_cmd
def supervise_recover(args):
    recovery_file = args.recovery_file
    return supervisor.recover_and_supervise(recovery_file)

@fiss_cmd
def entity_copy(args):
    """ Copy entities from one workspace to another. """
    if not args.to_workspace:
        args.to_workspace = args.workspace
    if not args.to_project:
        args.to_project = args.project
    if (args.project == args.to_project
        and args.workspace == args.to_workspace):
        eprint("destination project and namespace must differ from"
               " source workspace")
        return 1

    if not args.entities:
        # get a list of entities from source workspace matching entity_type
        ents = _entity_paginator(args.project, args.workspace, args.entity_type,
                                 page_size=500, filter_terms=None,
                                 sort_direction='asc')
        args.entities = [e['name'] for e in ents]

    prompt = "Copy {0} {1}(s) from {2}/{3} to {4}/{5}?\n[Y\\n]: "
    prompt = prompt.format(len(args.entities), args.entity_type, args.project,
                           args.workspace, args.to_project, args.to_workspace)

    if not args.yes and not _confirm_prompt("", prompt):
        return

    r = fapi.copy_entities(args.project, args.workspace, args.to_project,
                           args.to_workspace, args.entity_type, args.entities,
                           link_existing_entities=args.link)
    fapi._check_response_code(r, 201)
    return 0

@fiss_cmd
def proj_list(args):
    '''Retrieve the list of billing projects accessible to the caller/user, and
       show the level of access granted for each (e.g. Owner, User, ...)'''
    projects = fapi.list_billing_projects()
    fapi._check_response_code(projects, 200)
    projects = sorted(projects.json(), key=lambda d: d['projectName'])
    l = map(lambda p: '{0}\t{1}'.format(p['projectName'], p['role']), projects)
    # FIXME: add username col to output, for when iterating over multiple users
    return ["Project\tRole"] + l

@fiss_cmd
def config_validate(args):
    ''' Validate a workspace configuration: if an entity was specified (i.e.
        upon which the configuration should operate), then also validate that
        the entity has the necessary attributes'''

    r = fapi.validate_config(args.project, args.workspace, args.namespace,
                                                                args.config)
    fapi._check_response_code(r, 200)
    entity_d = None
    config_d = r.json()
    if args.entity:
        entity_type = config_d['methodConfiguration']['rootEntityType']
        entity_r = fapi.get_entity(args.project, args.workspace,
                                                 entity_type, args.entity)
        fapi._check_response_code(entity_r, [200,404])
        if entity_r.status_code == 404:
            eprint("Error: No {0} named '{1}'".format(entity_type, args.entity))
            return 2
        else:
            entity_d = entity_r.json()

    # also get the workspace info
    w = fapi.get_workspace(args.project, args.workspace)
    fapi._check_response_code(w, 200)
    workspace_d = w.json()

    ii, io, ma, mwa = _validate_helper(args, config_d, workspace_d, entity_d)
    ii_msg = "\nInvalid inputs:"
    io_msg = "\nInvalid outputs:"
    ma_msg = "\n{0} {1} doesn't satisfy the following inputs:".format(entity_type, args.entity) if args.entity else ""
    mwa_msg = "\nWorkspace {0}/{1} doesn't satisfy following inputs:".format(args.project, args.workspace)

    for errs, msg in zip([ii, io, ma, mwa], [ii_msg, io_msg, ma_msg, mwa_msg]):
        if errs:
            print(msg)
            for inp, val in errs:
                print("{0} -> {1}".format(inp, val))

    if ii + io + ma + mwa:
        return 1

def _validate_helper(args, config_d, workspace_d, entity_d=None):
    """ Return FISSFC validation information on config for a certain entity """
        # 4 ways to have invalid config:
    invalid_inputs = sorted(config_d["invalidInputs"])
    invalid_outputs = sorted(config_d["invalidOutputs"])

    # Also insert values for invalid i/o
    invalid_inputs = [(i, config_d['methodConfiguration']['inputs'][i]) for i in invalid_inputs]
    invalid_outputs = [(i, config_d['methodConfiguration']['outputs'][i]) for i in invalid_outputs]

    missing_attrs = []
    missing_wksp_attrs = []

    # If an entity was provided, also check to see if that entity has the necessary inputs
    if entity_d:
        entity_type = config_d['methodConfiguration']['rootEntityType']

        # If the attribute is listed here, it has an entry
        entity_attrs = set(entity_d['attributes'])

        # Optimization, only get the workspace attrs if the method config has any
        workspace_attrs = workspace_d['workspace']['attributes']

        # So now iterate over the inputs
        for inp, val in iteritems(config_d['methodConfiguration']['inputs']):
            # Must be an attribute on the entity
            if val.startswith("this."):
                # Normally, the value is of the form 'this.attribute',
                # but for operations on sets, e.g. one can also do
                # 'this.samples.attr'. But even in this case, there must be a
                # 'samples' attribute on the sample set, so checking for the middle
                # value works as expected. Other pathological cases would've been
                # caught above by the validation endpoint
                expected_attr = val.split('.')[1]
                # 'name' is special, it really means '_id', which everything has
                if expected_attr == "name":
                    continue
                if expected_attr not in entity_attrs:
                    missing_attrs.append((inp, val))

            if val.startswith("workspace."):
                # Anything not matching this format will be caught above
                expected_attr = val.split('.')[1]
                if expected_attr not in workspace_attrs:
                    missing_wksp_attrs.append((inp, val))
            # Anything else is a literal

    return invalid_inputs, invalid_outputs, missing_attrs, missing_wksp_attrs

@fiss_cmd
def runnable(args):
    """ Show me what can be run in a given workspace """
    w = fapi.get_workspace(args.project, args.workspace)
    fapi._check_response_code(w, 200)
    workspace_d = w.json()

    if args.config and args.namespace and not args.entity:
        # See what entities I can run on with this config
        r = fapi.validate_config(args.project, args.workspace, args.namespace,
                                                                    args.config)
        fapi._check_response_code(r, 200)
        config_d = r.json()



        # First validate without any sample sets
        errs = sum(_validate_helper(args, config_d, workspace_d, None), [])
        if errs:
            print("Configuration contains invalid expressions")
            return 1

        # Now get  all the possible entities, and evaluate each
        entity_type = config_d['methodConfiguration']['rootEntityType']
        ent_r = fapi.get_entities(args.project, args.workspace, entity_type)
        fapi._check_response_code(r, 200)
        entities = ent_r.json()

        can_run_on    = []
        cannot_run_on = []

        # Validate every entity
        for entity_d in entities:
            # If there are errors in the validation
            if sum(_validate_helper(args, config_d, workspace_d, entity_d), []):
                cannot_run_on.append(entity_d['name'])
            else:
                can_run_on.append(entity_d['name'])

        # Print what can be run
        if can_run_on:
            print("{0} CAN be run on {1} {2}(s):".format(args.config, len(can_run_on), entity_type))
            print("\n".join(can_run_on)+"\n")

        print("{0} CANNOT be run on {1} {2}(s)".format(args.config, len(cannot_run_on), entity_type))
            #print("\n".join(cannot_run_on))

    # See what method configs are possible for the given sample set
    elif args.entity and args.entity_type and not args.config:
        entity_r = fapi.get_entity(args.project, args.workspace,
                                   args.entity_type, args.entity)
        fapi._check_response_code(entity_r, [200,404])
        if entity_r.status_code == 404:
            print("Error: No {0} named '{1}'".format(args.entity_type, args.entity))
            return 2
        entity_d = entity_r.json()

        # Now get all the method configs in the workspace
        conf_r = fapi.list_workspace_configs(args.project, args.workspace)
        fapi._check_response_code(conf_r, 200)

        # Iterate over configs in the workspace, and validate against them
        for cfg in conf_r.json():
            # If we limit search to a particular namespace, skip ones that don't match
            if args.namespace and cfg['namespace'] != args.namespace:
                continue

            # But we have to get the full description
            r = fapi.validate_config(args.project, args.workspace,
                                    cfg['namespace'], cfg['name'])
            fapi._check_response_code(r, [200, 404])
            if r.status_code == 404:
                # Permission error, continue
                continue
            config_d = r.json()
            errs = sum(_validate_helper(args, config_d, workspace_d, entity_d),[])
            if not errs:
                print(cfg['namespace'] + "/" + cfg['name'])

    elif args.entity_type:
        # Last mode, build a matrix of everything based on the entity type
        # Get all of the entity_type
        ent_r = fapi.get_entities(args.project, args.workspace, args.entity_type)
        fapi._check_response_code(ent_r, 200)
        entities = ent_r.json()
        entity_names = sorted(e['name'] for e in entities)

        conf_r = fapi.list_workspace_configs(args.project, args.workspace)
        fapi._check_response_code(conf_r, 200)
        conf_list = conf_r.json()
        config_names = sorted(c['namespace'] + '/' + c['name'] for c in conf_list)
        mat = {c:dict() for c in config_names}

        # Now iterate over configs, building up the matrix
        # Iterate over configs in the workspace, and validate against them
        for cfg in conf_list:

            # If we limit search to a particular namespace, skip ones that don't match
            if args.namespace and cfg['namespace'] != args.namespace:
                continue
            # But we have to get the full description
            r = fapi.validate_config(args.project, args.workspace,
                                    cfg['namespace'], cfg['name'])
            fapi._check_response_code(r, [200, 404])
            if r.status_code == 404:
                # Permission error, continue
                continue
            config_d = r.json()

            # Validate against every entity
            for entity_d in entities:
                errs = sum(_validate_helper(args, config_d, workspace_d, entity_d),[])
                #TODO: True/False? Y/N?
                symbol = "X" if not errs else ""
                cfg_name = cfg['namespace'] + '/' + cfg['name']
                mat[cfg_name][entity_d['name']] = symbol

        # Now print the validation matrix
        # headers
        print("Namespace/Method Config\t" + "\t".join(entity_names))
        for conf in config_names:
            print(conf + "\t" + "\t".join(mat[conf][e] for e in entity_names))


    else:
        print("runnable requires a namespace+configuration or entity type")
        return 1

#################################################
# Utilities
#################################################

def _make_set_export_cmd(subparsers, parent_parsers, type, prefix):
    subp = subparsers.add_parser(prefix + '_export',
            parents = parent_parsers,
            description='Export a %s entity from a given workspace, returning '\
            'a list of lines in TSV form which completely defines the %s and '\
            'is suitable for reloading with entity_import.' % (type, type))
    subp.set_defaults(func=set_export, entity_type=type)

def _confirm_prompt(message, prompt="\nAre you sure? [y/yes (default: no)]: ",
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
        msg = "No project provided and no default project configured"
        raise argparse.ArgumentTypeError(msg)
    return value

def _entity_paginator(namespace, workspace, etype, page_size=500,
                            filter_terms=None, sort_direction="asc"):
    """Pages through the get_entities_query endpoint to get all entities in
       the workspace without crashing.
    """

    page = 1
    all_entities = []
    # Make initial request
    r = fapi.get_entities_query(namespace, workspace, etype, page=page,
                           page_size=page_size, sort_direction=sort_direction,
                           filter_terms=filter_terms)
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
                               filter_terms=filter_terms)
        fapi._check_response_code(r, 200)
        entities = r.json()['results']
        all_entities.extend(entities)
        page += 1

    return all_entities

def eprint(*args, **kwargs):
    """ Print a message to stderr """
    print(*args, file=sys.stderr, **kwargs)

def __cmd_to_func(cmd):
    """ Returns the function object in this module matching cmd. """
    fiss_module = sys.modules[__name__]
    # Returns None if string is not a recognized FISS command
    func = getattr(fiss_module, cmd, None)
    if func and not hasattr(func, 'fiss_cmd'):
        func = None
    return func

def _valid_headerline(l, model='firecloud'):
    """return true if the given string is a valid loadfile header"""

    if not l:
        return False
    headers = l.split('\t')
    first_col = headers[0]

    tsplit = first_col.split(':')
    if len(tsplit) != 2:
        return False

    if tsplit[0] in ('entity', 'update'):
        if model == 'flexible':
            return tsplit[1].endswith('_id')
        else:
            return tsplit[1] in ('participant_id', 'participant_set_id',
                                 'sample_id', 'sample_set_id',
                                 'pair_id', 'pair_set_id')
    elif tsplit[0] == 'membership':
        if len(headers) < 2:
            return False
        # membership:sample_set_id   sample_id, e.g.
        return tsplit[1].replace('set_', '') == headers[1]
    else:
        return False

def _batch_load(project, workspace, headerline, entity_data, chunk_size=500,
                model='firecloud'):
    """ Submit a large number of entity updates in batches of chunk_size """


    if fcconfig.verbosity:
        print("Batching " + str(len(entity_data)) + " updates to Firecloud...")

    # Parse the entity type from the first cell, e.g. "entity:sample_id"
    # First check that the header is valid
    if not _valid_headerline(headerline, model):
        eprint("Invalid loadfile header:\n" + headerline)
        return 1

    update_type = "membership" if headerline.startswith("membership") else "entity"
    etype = headerline.split('\t')[0].split(':')[1].replace("_id", "")

    # Split entity_data into chunks
    total = int(len(entity_data) / chunk_size) + 1
    batch = 0
    for i in range(0, len(entity_data), chunk_size):
        batch += 1
        if fcconfig.verbosity:
            print("Updating {0} {1}s {2}-{3}, batch {4}/{5}".format(
                etype, update_type, i+1, min(i+chunk_size, len(entity_data)),
                batch, total))
        this_data = headerline + '\n' + '\n'.join(entity_data[i:i+chunk_size])

        # Now push the entity data to firecloud
        r = fapi.upload_entities(project, workspace, this_data, model)
        fapi._check_response_code(r, 200)

    return 0

__PatternsToFilter = [
    # This provides a systematic way of turning complex FireCloud messages into
    # shorter, more comprehensible feedback: each filter entry has the form
    #  [regex_to_match, replacement_template, match_groups_to_fill_in_template]
    ['^(.+)SlickWorkspaceContext\(Workspace\(([^,]+),([^,]*).*$', '%s%s::%s', (1,2,3) ],
]
for i in range(len(__PatternsToFilter)):
    __PatternsToFilter[i][0] = re.compile(__PatternsToFilter[i][0])

def __pretty_print_fc_exception(e):
    # Look for integer error code, but fallback to the exception's classname
    preface = 'Error '
    code = getattr(e, "code", type(e).__name__)
    if fcconfig.verbosity:
        (_, _, trback) = sys.exc_info()
        print_traceback(trback)
    try:
        # Attempt to unpack error message as JSON
        e = json.loads(e.args[0])
        # Default to 'FireCloud' if component which gave error was not specified
        source = ' (' + e.get('source','FireCloud') + ')'
        msg = e['message']
        for pattern in __PatternsToFilter:
            match = pattern[0].match(msg)
            if match:
                msg = pattern[1] % (match.group(*(pattern[2])))
                break
    except Exception as je:
        # Could not unpack error to JSON, fallback to original message
        if isinstance(code, str):
            preface = ''
        source  = ''
        msg = "{0}".format(e)

    print("{0}{1}{2}: {3}".format(preface, code, source, msg))
    return 99

def printToCLI(value):
    retval = value if isinstance(value, int) else 0
    if isinstance(value, dict):
        # See attr_get for genesis of __header__
        header = value.pop("__header__", None)
        if header:
            print('\t'.join(header))
        for k, v in sorted(value.items()):
            print(u'{0}\t{1}'.format(k, v))
    elif isinstance(value, (list, tuple)):
        list(map(lambda v: print(v), value))
    elif not isinstance(value, int):
        if isinstance(value, text_type):
            print(value)
        else:
            print(u("{0}".format(value)))
    return retval

#################################################
# Main entrypoints
#################################################

def main(argv=None):
    # Use this entry point to call high level api and have objects returned,
    # (see firecloud/tests/highlevel_tests.py:call_func for usage examples)
    if not argv:
        argv = sys.argv

    proj_required = not bool(fcconfig.project)
    meth_ns_required = not bool(fcconfig.method_ns)
    workspace_required = not bool(fcconfig.workspace)
    etype_required = not bool(fcconfig.entity_type)
    etype_choices = ['participant', 'participant_set', 'sample', 'sample_set',
                     'pair', 'pair_set' ]

    # Initialize core parser (TODO: Add longer description)
    usage  = 'fissfc [OPTIONS] [CMD [-h | arg ...]]'
    epilog = 'positional arguments:\n' + \
             '  CMD [-h | arg ...]    Command and arguments to run.'
    parser = argparse.ArgumentParser(description='FISS: The FireCloud CLI',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     usage=usage, epilog=epilog)
    # Core Flags
    parser.add_argument('-u', '--url', dest='api_url', default=None,
            help='Firecloud API root URL [default: %s]' % fcconfig.root_url)
    
    parser.add_argument('-c', '--credentials', default=None,
                        help='Firecloud credentials file')

    parser.add_argument("-v", "--version", action='version',version=__version__)

    parser.add_argument('-V', '--verbose', action='count', default=0,
        help='Emit progressively more detailed feedback during execution, '
             'e.g. to confirm when actions have completed or to show URL '
             'and parameters of REST calls.  Multiple -V may be given.')

    parser.add_argument("-y", "--yes", action='store_true',
                help="Assume yes for any prompts")
    parser.add_argument("-l", "--list", nargs='?', metavar="CMD",
                        help="list or search available commands and exit")
    parser.add_argument("-F", "--function", nargs='+', metavar="CMD",
                        help="Show the code for the given command(s) and exit")

    # Many commands share arguments, and we can make parent parsers to make it
    # easier to reuse arguments. Commands that operate on workspaces
    # all take a (google) project and a workspace name

    workspace_parent = argparse.ArgumentParser(add_help=False)
    workspace_parent.add_argument('-w', '--workspace',
        default=fcconfig.workspace, required=workspace_required,
        help='Workspace name (required if no default workspace configured)')

    proj_help = 'Project (workspace namespace). Required if no default ' \
                'project was configured'
    workspace_parent.add_argument('-p', '--project', default=fcconfig.project,
                        help=proj_help, required=proj_required)

    dest_space_parent = argparse.ArgumentParser(add_help=False)
    dest_space_parent.add_argument("-P", "--to-project",
                               help="Project (Namespace) of clone workspace")
    # FIXME: change to --tospace
    dest_space_parent.add_argument("-W", "--to-workspace",
                               help="Name of clone workspace")

    # Commands that update ACL roles require a role and list of users
    acl_parent = argparse.ArgumentParser(add_help=False)
    acl_parent.add_argument('-r', '--role', help='ACL role', required=True,
                           choices=['OWNER', 'READER', 'WRITER', 'NO ACCESS'])
    acl_parent.add_argument('--users', nargs='+', required=True, metavar='user',
        help='FireCloud usernames. Use "public" to set global permissions.')

    # Commands that operates on entity_types
    etype_parent = argparse.ArgumentParser(add_help=False)
    etype_help = \
        "Entity type, required if no default entity_type was configured"
    etype_parent.add_argument('-t', '--entity-type', required=etype_required,
                              default=fcconfig.entity_type, help=etype_help)

    # Commands that require an entity name
    entity_parent = argparse.ArgumentParser(add_help=False)
    entity_parent.add_argument('-e', '--entity', required=True,
                               help="Entity name (required)")

    # Commands that work with methods
    meth_parent = argparse.ArgumentParser(add_help=False)
    meth_parent.add_argument('-m', '--method', required=True,
                             help='method name')
    meth_parent.add_argument('-n', '--namespace', help='Method namespace',
                             default=fcconfig.method_ns,
                             required=meth_ns_required)

    # Commands that work with method configurations
    conf_parent = argparse.ArgumentParser(add_help=False)
    conf_parent.add_argument('-c', '--config', required=True,
                             help='Method config name')
    conf_parent.add_argument('-n', '--namespace', default=fcconfig.method_ns,
                             help='Method config namespace',
                             required=meth_ns_required)

    # Commands that need a snapshot_id
    snapshot_parent = argparse.ArgumentParser(add_help=False)
    snapshot_parent.add_argument('-i', '--snapshot-id', required=True,
                                 help="Snapshot ID (version) of method/config")

    # Commands that take an optional list of attributes
    attr_parent = argparse.ArgumentParser(add_help=False)
    attr_parent.add_argument('-a', '--attributes', nargs='*', metavar='attr',
                             help='List of attributes')

    # Create one subparser for each fiss equivalent
    subparsers = parser.add_subparsers(prog='fissfc [OPTIONS]',
                                       help=argparse.SUPPRESS)

    # Create Workspace
    subp = subparsers.add_parser('space_new', parents=[workspace_parent],
                                 description='Create new workspace')
    phelp = 'Limit access to the workspace to a specific authorization ' \
            'domain. For dbGaP-controlled access (domain name: ' \
            'dbGapAuthorizedUsers) you must have linked NIH credentials to ' \
            'your account.'
    subp.add_argument('--authdomain', default="", help=phelp)
    subp.set_defaults(func=space_new)

    # Determine existence of workspace
    subp = subparsers.add_parser('space_exists', parents=[workspace_parent],
        description='Determine if given workspace exists in given project')
    phelp = 'Do not print message, only return numeric status'
    subp.add_argument('-q', '--quiet', action='store_true', help=phelp)
    subp.set_defaults(func=space_exists)

    # Delete workspace
    subp = subparsers.add_parser('space_delete', description='Delete workspace')
    subp.add_argument('-w', '--workspace', help='Workspace name', required=True)
    subp.add_argument('-p', '--project', default=fcconfig.project,
                      help=proj_help, required=proj_required)
    subp.set_defaults(func=space_delete)

    # Get workspace information
    subp = subparsers.add_parser('space_info', parents=[workspace_parent],
                                 description='Show workspace information')
    subp.set_defaults(func=space_info)

    # List workspaces
    subp = subparsers.add_parser('space_list',
            description='List available workspaces in projects (namespaces) ' +
                        'to which you have access. If you have a config ' +
                        'file which defines a default project, then only ' +
                        'the workspaces in that project will be listed.')
    subp.add_argument('-p', '--project', default=fcconfig.project,
            help='List spaces for projects whose names start with this ' +
            'prefix. You may also specify . (a dot), to list everything.')
    subp.set_defaults(func=space_list)

    # Lock workspace
    subp = subparsers.add_parser('space_lock', description='Lock a workspace',
                                 parents=[workspace_parent])
    subp.set_defaults(func=space_lock)

    # Unlock Workspace
    subp = subparsers.add_parser('space_unlock', parents=[workspace_parent],
                                 description='Unlock a workspace')
    subp.set_defaults(func=space_unlock)

    # Clone workspace
    clone_desc = 'Clone a workspace. The destination namespace or name must ' \
                 'be different from the workspace being cloned'
    subp = subparsers.add_parser('space_clone', description=clone_desc,
                                 parents=[workspace_parent, dest_space_parent])
    subp.set_defaults(func=space_clone)

    # Import data into a workspace
    subp = subparsers.add_parser('entity_import', parents=[workspace_parent],
                                 description='Import data into a workspace')
    subp.add_argument('-f','--tsvfile', required=True,
                      help='Tab-delimited loadfile')
    subp.add_argument('-C', '--chunk-size', default=500, type=int,
                      help='Maximum entities to import per api call')
    subp.add_argument('-m', '--model', default='firecloud',
                      choices=['firecloud', 'flexible'], help='Data Model ' +
                      'type. "%(default)s" (default) or "flexible"')
    subp.set_defaults(func=entity_import)

    # Export data (set/container entity) from workspace
    export_cmd_args = [workspace_parent, entity_parent]
    _make_set_export_cmd(subparsers, export_cmd_args, 'sample_set', 'sset')
    _make_set_export_cmd(subparsers, export_cmd_args, 'pair_set', 'pset')
    _make_set_export_cmd(subparsers, export_cmd_args, 'participant_set','ptset')

    # List of entity types in a workspace
    subp = subparsers.add_parser(
        'entity_types', parents=[workspace_parent],
        description='List entity types in a workspace')
    subp.set_defaults(func=entity_types)

    # List of entities in a workspace
    subp = subparsers.add_parser(
        'entity_list', description='List entity types in a workspace',
        parents=[workspace_parent])
    subp.set_defaults(func=entity_list)

    # List of entities in a workspace
    subp = subparsers.add_parser(
        'entity_tsv', description='Get list of entities in TSV format. ' +
        'Download files for which the encoding is undetected (e.g. ZIP ' +
        'archives).',
        parents=[workspace_parent, etype_parent])
    subp.add_argument('-a', '--attrs', nargs='*',
                      help='list of ordered attribute names')
    subp.add_argument('-m', '--model', default='firecloud',
                      choices=['firecloud', 'flexible'], help='Data Model ' +
                      'type. "%(default)s" (default) or "flexible"')
    subp.set_defaults(func=entity_tsv)
    
    # List of participants
    subp = subparsers.add_parser(
        'participant_list',
        parents=[workspace_parent],
        description='Return list of participants within a given container, '\
            'which by default is the workspace; otherwise, participants in '\
            'the named entity will be listed.  If an entity is named but no ' \
            'type is given, then participant_set is assumed. The containers ' \
            'supported are: participant, participant_set, workspace'
            )
    subp.add_argument('-e', '--entity', default=None,
            help='Entity name, to list participants within container entities')
    subp.add_argument('-t', '--entity-type', default='participant_set',
            help='The type for named entity [default:%(default)s]`')
    subp.set_defaults(func=participant_list)

    # List of pairs
    subp = subparsers.add_parser(
        'pair_list',
        parents=[workspace_parent],
        description='Return the list of pairs within a given container, ' \
            'which by default is the workspace; otherwise, the pairs within '\
            'the named entity will be listed.  If an entity is named but no '\
            'type is given, then pair_set will be assumed. The containers '\
            'supported are: pair, pair_set, participant, workspace')
    subp.add_argument('-e', '--entity', default=None,
            help='Entity name, to list pairs within container entities')
    subp.add_argument('-t', '--entity-type', default='pair_set',
            help='The type for named entity [default:%(default)s]`')
    subp.set_defaults(func=pair_list)

    # List of samples
    subp = subparsers.add_parser(
        'sample_list',
        parents=[workspace_parent],
        description='Return the list of samples within a given container, ' \
            'which by default is the workspace; otherwise, the samples within '\
            'the named entity will be listed.  If an entity is named but no '\
            'type is not given, then sample_set is assumed. The containers '\
            'supported are:\n'\
            'sample, sample_set, pair, participant, workspace')
    subp.add_argument('-e', '--entity', default=None,
            help='Entity name, to list samples within container entities')
    subp.add_argument('-t', '--entity-type', default='sample_set',
            help='The type for named entity [default:%(default)s]`')
    subp.set_defaults(func=sample_list)

    # List of sample sets
    subp = subparsers.add_parser(
        'sset_list', description='List sample sets in a workspace',
        parents=[workspace_parent])
    subp.set_defaults(func=sset_list)

    # Delete entity in a workspace
    subp = subparsers.add_parser(
        'entity_delete', description='Delete entity in a workspace',
        parents=[workspace_parent, etype_parent, entity_parent])
    subp.set_defaults(func=entity_delete)

    subp = subparsers.add_parser(
        'participant_delete', description='Delete participant in a workspace',
        parents=[workspace_parent, entity_parent])
    subp.set_defaults(func=participant_delete)

    subp = subparsers.add_parser(
        'sample_delete', description='Delete sample in a workspace',
        parents=[workspace_parent, entity_parent])
    subp.set_defaults(func=sample_delete)

    subp = subparsers.add_parser(
        'sset_delete', description='Delete sample set in a workspace',
        parents=[workspace_parent, entity_parent])
    subp.set_defaults(func=sset_delete)

    # Show workspace roles
    subp = subparsers.add_parser(
        'space_acl', description='Show users and roles in workspace',
        parents=[workspace_parent])
    subp.set_defaults(func=space_acl)

    # Set workspace roles
    subp = subparsers.add_parser('space_set_acl',
        description='Show users and roles in workspace',
        parents=[workspace_parent, acl_parent])
    subp.set_defaults(func=space_set_acl)

    # Push a new workflow to the methods repo
    subp = subparsers.add_parser('meth_new', parents=[meth_parent],
        description='Install a method definition to the repository')
    subp.add_argument('-d','--wdl', required=True,
                      help='Method definiton, as a file of WDL (Workflow ' +
                           'Description Language)')
    subp.add_argument('-s', '--synopsis',
                      help='Short (<80 chars) description of method')
    subp.add_argument('--doc', help='Optional documentation file <10Kb')
    subp.add_argument('-c', '--comment', metavar='SNAPSHOT_COMMENT',
                      help='Optional comment specific to this snapshot',
                      default='')
    subp.set_defaults(func=meth_new)

    # Redact a method
    subp = subparsers.add_parser('meth_delete',
        description='Redact method from the methods repository',
        parents=[meth_parent, snapshot_parent])
    subp.set_defaults(func=meth_delete)
    
    # Retreive the WDL of a method
    subp = subparsers.add_parser('meth_wdl',
        description='Retrieve the WDL of a method',
        parents=[meth_parent, snapshot_parent])
    subp.set_defaults(func=meth_wdl)

    # Access control list operations (upon methods)
    # Get ACL
    subp = subparsers.add_parser('meth_acl',
        description='Show users and roles for a method',
        parents=[meth_parent, snapshot_parent])
    subp.set_defaults(func=meth_acl)

    # Set ACL
    subp = subparsers.add_parser('meth_set_acl',
        description='Assign an ACL role to a list of users for a workflow',
        parents=[meth_parent, acl_parent])
    subp.add_argument('-i', '--snapshot-id',
                      help="Snapshot ID (version) of method/config")
    subp.set_defaults(func=meth_set_acl)

    # List available methods
    subp = subparsers.add_parser('meth_list',
                                 description='List available workflows')
    subp.add_argument('-m', '--method', default=None,
                      help='name of single workflow to search for (optional)')
    subp.add_argument('-n', '--namespace', default=None,
                      help='name of single workflow to search for (optional)')
    subp.add_argument('-i', '--snapshot-id', default=None,
                      help="Snapshot ID (version) of method/config")
    subp.set_defaults(func=meth_list)

    subp = subparsers.add_parser('meth_exists',
        description='Determine if named workflow exists in method repository')
    subp.add_argument('method', help='name of method to search for in repository')
    subp.set_defaults(func=meth_exists)

    # Configuration: list
    subp = subparsers.add_parser(
        'config_list', description='List available configurations')
    subp.add_argument('-w', '--workspace', help='Workspace name')
    subp.add_argument('-p', '--project', default=fcconfig.project,
                      help=proj_help)
    subp.add_argument('-c', '--config', default=None,
                      help='name of single workflow to search for (optional)')
    subp.add_argument('-n', '--namespace', default=None,
                      help='name of single workflow to search for (optional)')
    subp.add_argument('-i', '--snapshot-id', default=None,
                      help="Snapshot ID (version) of config (optional)")
    subp.set_defaults(func=config_list)

    # Configuration: delete
    subp = subparsers.add_parser('config_delete', parents=[conf_parent],
                                 description='Delete a workspace configuration')
    subp.add_argument('-w', '--workspace', help='Workspace name',
                      default=fcconfig.workspace, required=workspace_required)
    subp.add_argument('-p', '--project', default=fcconfig.project,
                      help=proj_help, required=proj_required)
    subp.set_defaults(func=config_delete)

    # Method configuration commands
    subp = subparsers.add_parser('config_get',
        description='Retrieve method configuration definition',
        parents=[conf_parent])
    subp.add_argument('-w', '--workspace', help='Workspace name',
                      default=fcconfig.workspace, required=workspace_required)
    subp.add_argument('-p', '--project', default=fcconfig.project,
                      help=proj_help, required=proj_required)
    subp.set_defaults(func=config_get)
    
    subp = subparsers.add_parser('config_wdl',
        description='Retrieve method configuration WDL',
        parents=[conf_parent])
    subp.add_argument('-w', '--workspace', help='Workspace name',
                      default=fcconfig.workspace, required=workspace_required)
    subp.add_argument('-p', '--project', default=fcconfig.project,
                      help=proj_help, required=proj_required)
    subp.set_defaults(func=config_wdl)
    
    subp = subparsers.add_parser('config_diff',
        description='Compare method configuration definitions across workspaces',
        parents=[conf_parent])
    subp.add_argument('-w', '--workspace', help='First Workspace name',
                      default=fcconfig.workspace, required=workspace_required)
    subp.add_argument('-p', '--project', default=fcconfig.project,
                      help="First " + proj_help, required=proj_required)
    subp.add_argument('-C', '--Config', help="Second method config name")
    subp.add_argument('-N', '--Namespace', help="Second method config namespace")
    subp.add_argument('-W', '--Workspace', help='Second Workspace name',
                      default=fcconfig.workspace, required=workspace_required)
    subp.add_argument('-P', '--Project', default=fcconfig.project,
                      help="Second " + proj_help, required=proj_required)
    subp.set_defaults(func=config_diff)

    subp = subparsers.add_parser('config_copy', description=
        'Copy a method config to a new name/space/namespace/project, ' +
        'at least one of which MUST be specified.', parents=[conf_parent])
    subp.add_argument('-p', '--fromproject', default=fcconfig.project,
                      help=proj_help, required=proj_required)
    subp.add_argument('-s', '--fromspace', help='from workspace',
                      default=fcconfig.workspace, required=workspace_required)
    subp.add_argument('-C', '--toname', help='name of the copied config')
    subp.add_argument('-S', '--tospace', help='destination workspace')
    subp.add_argument("-N", "--tonamespace", help="destination namespace")
    subp.add_argument("-P", "--toproject", help="destination project")
    subp.set_defaults(func=config_copy)

    subp = subparsers.add_parser('config_new', description=config_new.__doc__,
        parents=[meth_parent, snapshot_parent, workspace_parent, etype_parent])
    subp.add_argument('-c', '--configname', default=None,
        help='name of new config; if unspecified, method name will be used')
    subp.set_defaults(func=config_new)

    subp = subparsers.add_parser('config_template',
                                 parents=[meth_parent, snapshot_parent],
                                 description='Generate a template method ' +
                                             'configuration, from the given ' +
                                             'repository method')
    subp.add_argument('-c', '--configname', default=None,
        help='name of new config; if unspecified, method name will be used')
    subp.add_argument('-t', '--entity-type', required=False, default='',
        help='Root entity type, over which method config will execute')
    subp.set_defaults(func=config_template)

    subp = subparsers.add_parser('config_put', parents=[workspace_parent],
                                 description=config_put.__doc__)
    subp.add_argument('-c', '--config', required=True,
        help='Method configuration definition, as described above')
    subp.set_defaults(func=config_put)

    subp = subparsers.add_parser('config_acl',
        description='Show users and roles for a method configuration',
        parents=[conf_parent, snapshot_parent])
    subp.set_defaults(func=config_acl)

    # FIXME: continue subp = ... meme below, instead of uniquely naming each
    #        subparse; better yet, most of this can be greatly collapsed and
    #        pushed into a separate function and/or auto-generated

    # Set ACL
    subp = subparsers.add_parser('config_set_acl', description='Assign an ' +
                                 'ACL role to a list of users for a  config',
                                 parents=[conf_parent, acl_parent])
    subp.add_argument('-i', '--snapshot-id',
                      help="Snapshot ID (version) of method/config")
    subp.set_defaults(func=config_set_acl)

    # Status
    subp = subparsers.add_parser('health',
                                 description='Show health of FireCloud services')
    subp.set_defaults(func=health)

    subp = subparsers.add_parser('attr_get',
        description='Retrieve attribute values from an entity identified by ' +
        'name and type.  If either name or type are omitted then workspace ' +
        'attributes will be returned.',
        parents=[workspace_parent, attr_parent])

    # etype_parent not used for attr_get, because entity type is optional
    subp.add_argument('-t', '--entity-type', choices=etype_choices, default='',
                      required=False, help='Entity type to retrieve ' +
                                           'annotations from.')
    subp.add_argument('-e', '--entity',
                      help="Entity to retrieve attributes from")
    subp.set_defaults(func=attr_get)

    subp = subparsers.add_parser('attr_set', parents=[workspace_parent],
                                 description="Set attributes on a workspace")
    subp.add_argument('-a', '--attribute', required=True, metavar='attr',
                      help='Name of attribute to set')
    subp.add_argument('-v', '--value', required=True, help='Attribute value')
    subp.add_argument('-t', '--entity-type', choices=etype_choices,
                      required=etype_required, default=fcconfig.entity_type,
                      help=etype_help)
    subp.add_argument('-e', '--entity', help="Entity to set attribute on")
    subp.set_defaults(func=attr_set)

    subp = subparsers.add_parser('attr_list', parents=[workspace_parent],
        description='Retrieve names of attributes attached to given entity. ' +
                    'If no entity Type+Name is given, workspace-level ' +
                    'attributes will be listed.')
    # FIXME: this should explain that default entity is workspace
    subp.add_argument('-e', '--entity', help="Entity name")
    subp.add_argument('-t', '--entity-type', choices=etype_choices,
                      required=etype_required, default=fcconfig.entity_type,
                      help=etype_help)
    subp.set_defaults(func=attr_list)

    # Copy attributes
    subp = subparsers.add_parser(
        'attr_copy', description="Copy workspace attributes between workspaces",
        parents=[workspace_parent, dest_space_parent, attr_parent])
    subp.set_defaults(func=attr_copy)

    # Delete attributes
    subp = subparsers.add_parser(
        'attr_delete', description="Delete attributes in a workspace",
        parents=[workspace_parent, attr_parent])
    subp.add_argument('-t', '--entity-type', choices=etype_choices,
                      required=etype_required, default=fcconfig.entity_type,
                      help=etype_help)
    subp.add_argument('-e', '--entities', nargs='*', help='FireCloud entities')
    subp.set_defaults(func=attr_delete)

    # Set null sentinel values
    subp = subparsers.add_parser(
        'attr_fill_null', parents=[workspace_parent, etype_parent, attr_parent],
        description='Assign NULL sentinel value to attributes')
    subp.add_argument("-o", "--to-loadfile", metavar='loadfile',
                      help="Save changes to provided loadfile, but do not " +
                           "perform update")
    subp.set_defaults(func=attr_fill_null)

    # Delete unreferenced files from a workspace's bucket
    subp = subparsers.add_parser(
        'mop', description='Remove unused files from a workspace\'s bucket',
        parents=[workspace_parent])
    subp.add_argument('--dry-run', action='store_true',
                      help='Show deletions that would be performed')
    subp.set_defaults(func=mop)

    subp = subparsers.add_parser('noop',
                                 description='Simple no-op command, for ' +
                                             'exercising interface')
    subp.set_defaults(func=noop, proj=fcconfig.project, space=fcconfig.workspace)

    subp = subparsers.add_parser('config',
        description='Display value(s) of one or more configuration variables')
    subp.add_argument('variables', nargs='*',
        help='Name of configuration variable(s) (e.g. workspace, project). '
             'If no name is given, all config variables will be displayed.')
    subp.set_defaults(func=config_cmd)

    # Invoke a method configuration
    subp = subparsers.add_parser('config_start',
        description='Start running workflow in a given space',
        parents=[workspace_parent, conf_parent, entity_parent])
    # Duplicate entity type here since we want sample_set to be default
    subp.add_argument('-t', '--entity-type', default='sample_set',
                      choices=etype_choices,
                      help='Entity type to assign null values, if attribute ' +
                           'is missing. Default: %(default)s')
    expr_help = "(optional) Entity expression to use when entity type " \
                "doesn't match the method configuration." \
                "Example: 'this.samples'"
    subp.add_argument('-x', '--expression', help=expr_help, default='')
    subp.add_argument('-C', '--cache', default=True,
        help='boolean: use previously cached results if possible [%(default)s]')
    subp.set_defaults(func=config_start)
    
    # Abort a running method configuration
    subp = subparsers.add_parser('config_stop',
        description='Stop running submission ID in a given space',
        parents=[workspace_parent])
    subp.add_argument('-i', '--submission_id', required=True)
    subp.set_defaults(func=config_stop)

    # Loop over sample sets, performing a command
    ssloop_help = 'Loop over sample sets in a workspace, performing <action>'
    subp = subparsers.add_parser(
        'sset_loop', description=ssloop_help,
        parents=[workspace_parent, attr_parent])
    subp.add_argument('action', help='FISS command to execute')
    subp.add_argument('-c', '--config',
                               help='Method configuration name')
    subp.add_argument('-n', '--namespace',
                               help='Method configuration namespace')
    khelp = "Loop through all sample sets, ignoring errors"
    subp.add_argument('-k', '--keep-going', action='store_true',
                               help=khelp)
    subp.add_argument('-x', '--expression', help=expr_help)
    subp.set_defaults(func=sset_loop)

    subp = subparsers.add_parser('monitor', help="Monitor submitted jobs.",
        parents=[workspace_parent])
    subp.set_defaults(func=monitor)

    # Supervisor mode
    sup_help = "Run a Firehose-style workflow of workflows specified in DOT"
    subp = subparsers.add_parser('supervise', description=sup_help,
                                 parents=[workspace_parent])
    subp.add_argument('workflow', help='Workflow description in DOT')
    subp.add_argument('-n', '--namespace', default=fcconfig.method_ns,
                      required=meth_ns_required,
                      help='Methods namespace')
    subp.add_argument('-s', '--sample-sets', nargs='+',
                      help='Sample sets to run workflow on')
    jhelp = "File to save monitor data. This file can be passed to " + \
            "fissfc supervise_recover in case the supervisor crashes " + \
            "(Default: %(default)s)"
    recovery = os.path.expanduser('~/.fiss/monitor_data.json')
    subp.add_argument('-j', '--json-checkpoint', default=recovery,
                      help=jhelp)
    subp.set_defaults(func=supervise)

    # Recover an old supervisor
    rec_help = "Recover a supervisor submission from the checkpoint file"
    subp = subparsers.add_parser('supervise_recover', description=rec_help)
    subp.add_argument('recovery_file', default=recovery, nargs='?',
                            help='File where supervisor metadata was stored')
    subp.set_defaults(func=supervise_recover)

    # Space search
    subp = subparsers.add_parser(
        'space_search', description="Search for workspaces"
    )
    subp.add_argument('-b', '--bucket', help='Regex to match bucketName')
    subp.set_defaults(func=space_search)

    # Entity copy
    subp = subparsers.add_parser(
        'entity_copy', description='Copy entities from one workspace to another',
        parents=[workspace_parent, dest_space_parent, etype_parent])
    subp.add_argument('-e', '--entities', nargs='+', metavar='entity',
        help='Entities to copy. If omitted, all entities will be copied.')
    subp.add_argument('-l', '--link', action='store_true',
                      help='link new entities to existing entities')
    subp.set_defaults(func=entity_copy)


    # List billing projects
    subp = subparsers.add_parser('proj_list',
                                 description="List available billing projects")
    subp.set_defaults(func=proj_list)

    # Validate config
    subp = subparsers.add_parser('config_validate', parents=[workspace_parent],
        description="Validate a workspace configuration")
    subp.add_argument('-e', '--entity',
        help="Validate config against this entity. Entity is assumed to be " +
             "the same type as the config's root entity type")
    subp.add_argument('-c', '--config',
                               help='Method configuration name')
    subp.add_argument('-n', '--namespace',
                               help='Method configuration namespace')
    subp.set_defaults(func=config_validate)

    subp = subparsers.add_parser('runnable', parents=[workspace_parent],
        description="Show what configurations can be run on which entities.")
    subp.add_argument('-c', '--config',
        help='Method configuration name')
    subp.add_argument('-n', '--namespace',
        help='Method configuration namespace')
    subp.add_argument('-e', '--entity',
        help="Show me what configurations can be run on this entity")
    subp.add_argument('-t', '--entity-type', choices=etype_choices,
                      required=etype_required, default=fcconfig.entity_type,
                      help=etype_help)
    subp.set_defaults(func=runnable)

    # Create the .fiss directory if it doesn't exist
    fiss_home = os.path.expanduser("~/.fiss")
    if not os.path.isdir(fiss_home):
        os.makedirs(fiss_home)

    result = None

    # Special cases, print help with no arguments
    if len(argv) == 1:
        parser.print_help()
    elif argv[1] in ('-l', '--list'):
        # Print commands in a more readable way
        choices=[]
        for a in parser._actions:
            if isinstance(a, argparse._SubParsersAction):
                for choice, _ in a.choices.items():
                    choices.append(choice)

        # next arg is search term, if specified
        search = ''
        if len(argv) > 2:
            search = argv[2]
        result = list(filter(lambda c: search in c, sorted(choices)))
    elif argv[1] in ('-F', '--function'):
        # Show source for remaining args
        for fname in argv[2:]:
            # Get module name
            fiss_module = sys.modules[__name__]
            try:
                func = getattr(fiss_module, fname)
                result = u(''.join(getsourcelines(func)[0]))
            except AttributeError:
                result = None
    else:
        # Otherwise parse args & call correct subcommand (skipping argv[0])
        args = parser.parse_args(argv[1:])

        # Ensure CLI flags have greatest precedence (e.g. over config file)
        if args.verbose:
            fcconfig.set_verbosity(args.verbose)
        if args.api_url:
            fcconfig.set_root_url(args.api_url)
        if args.credentials:
            fcconfig.set_credentials(args.credentials)

        result = args.func(args)
        if result is None:
            result = 0

    return result

def main_as_cli(argv=None):
    '''Use this entry point to call HL fiss funcs as though from the UNIX CLI.
       (see firecloud/tests/highlevel_tests.py:call_cli for usage examples)'''
    try:
        result = main(argv)
    except Exception as e:
        result = __pretty_print_fc_exception(e)
    # FIXME: we should invert True/False return values to 0/1 here, to comply
    # with UNIX exit code semantics (and avoid problems with make, scripts, etc)
    return printToCLI(result)

if __name__ == '__main__':
    sys.exit( main_as_cli() )
