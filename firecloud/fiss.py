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
from inspect import getsourcelines
import argparse
import subprocess
import re

from six import iteritems, string_types, itervalues, u
from six.moves import input

from firecloud import api as fapi
from firecloud.fccore import *
from firecloud.errors import *
from firecloud.__about__ import __version__
from firecloud import supervisor
from firecloud.attrdict import *

fcconfig = fc_config_parse()

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

    r = fapi.list_workspaces(args.api_url)
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
        r = fapi.get_workspace(args.project, args.workspace, args.api_url)
        fapi._check_response_code(r, 200)
        exists = True
    except FireCloudServerError as e:
        if e.code == 404:
            exists = False
        else:
            raise
    if fapi.get_verbosity():
        result = "DOES NOT" if not exists else "DOES"
        eprint('Space <%s> %s exist in project <%s>' % (args.workspace, result, args.project))
    return exists

@fiss_cmd
def space_lock(args):
    """  Lock a workspace """
    r = fapi.lock_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 204)
    if fapi.get_verbosity():
        eprint('Locked workspace {0}/{1}'.format(args.project, args.workspace))
    return 0

@fiss_cmd
def space_unlock(args):
    """ Unlock a workspace """
    r = fapi.unlock_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 204)
    if fapi.get_verbosity():
        eprint('Unlocked workspace {0}/{1}'.format(args.project,args.workspace))
    return 0

@fiss_cmd
def space_new(args):
    """ Create a new workspace. """
    r = fapi.create_workspace(args.project, args.workspace,
                                 args.authdomain, dict(), args.api_url)
    fapi._check_response_code(r, 201)
    if fapi.get_verbosity():
        eprint(r.content)
    return 0

@fiss_cmd
def space_info(args):
    """ Get metadata for a workspace. """
    r = fapi.get_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)
    #TODO?: pretty_printworkspace(c)
    return r.content

@fiss_cmd
def space_delete(args):
    """ Delete a workspace. """
    message = "WARNING: this will delete workspace: \n\t{0}/{1}".format(
        args.project, args.workspace)
    if not args.yes and not _confirm_prompt(message):
        return 0

    r = fapi.delete_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, [202, 404])
    if fapi.get_verbosity():
        print('Deleted workspace {0}/{1}'.format(args.project, args.workspace))
    return 0

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
    print(msg)

@fiss_cmd
def entity_import(args):
    """ Upload an entity loadfile. """
    project = args.project
    workspace = args.workspace
    chunk_size = args.chunk_size
    api_url = args.api_url
    verbose = args.verbose

    with open(args.tsvfile) as tsvf:
        headerline = tsvf.readline().strip()
        entity_data = [l.strip() for l in tsvf]

    return _batch_load(project, workspace, headerline, entity_data,
                                        chunk_size, api_url, verbose)

@fiss_cmd
def entity_types(args):
    """ List entity types in a workspace """
    r = fapi.list_entity_types(args.project, args.workspace,
                               args.api_url)
    fapi._check_response_code(r, 200)
    return r.json().keys()

@fiss_cmd
def entity_list(args):
    """ List entities in a workspace. """
    r = fapi.get_entities_with_type(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)
    return [ '{0}\t{1}'.format(e['entityType'], e['name']) for e in r.json() ]

# REMOVED: This now returns a *.zip* file containing two tsvs, which is far
# less useful for FISS users...
# def entity_tsv(args):
#     """ Get list of entities in TSV format. """
#     r = fapi.get_entities_tsv(args.project, args.workspace,
#                               args.entity_type, args.api_url)
#     fapi._check_response_code(r, 200)
#
#     print(r.content)

@fiss_cmd
def participant_list(args):
    """ List participants in a workspace. """
    r = fapi.get_entities(args.project, args.workspace,
                          "participant", args.api_url)
    fapi._check_response_code(r, 200)
    for entity in r.json():
        print(entity['name'])

@fiss_cmd
def sample_list(args):
    """ List samples in a workspace. """
    r = fapi.get_entities(args.project, args.workspace,
                             "sample", args.api_url)
    fapi._check_response_code(r, 200)
    for entity in r.json():
        print(entity['name'])

@fiss_cmd
def sset_list(args):
    """ List sample sets in a workspace """
    r = fapi.get_entities(args.project, args.workspace,
                          "sample_set", args.api_url)
    fapi._check_response_code(r, 200)

    for entity in r.json():
        print(entity['name'])

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
def space_acl(args):
    """ Get Access Control List for a workspace."""
    r = fapi.get_workspace_acl(args.project, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)
    acl = r.json()['acl']
    for user in sorted(acl):
        print('{0}\t{1}'.format(user, acl[user]['accessLevel']))

@fiss_cmd
def space_set_acl(args):
    """ Assign an ACL role to list of users for a workspace """
    acl_updates = [{"email": user,
                   "accessLevel": args.role} for user in args.users]
    r = fapi.update_workspace_acl(args.project, args.workspace,
                                  acl_updates, args.api_url)
    fapi._check_response_code(r, 200)
    update_info = r.json()

    if len(update_info['usersNotFound']) == 0:
        print("Successfully updated {0} role(s)".format(len(acl_updates)))
    else:
        print("Unable to assign role to the following users (usernames not found):")
        for u_info in update_info['usersNotFound']:
            print(u_info['email'])
        return 1

@fiss_cmd
def flow_new(args):
    """ Submit a new workflow (or update) to the methods repository. """
    r = fapi.update_repository_method(args.namespace, args.method, args.synopsis,
                                      args.wdl, args.doc, args.api_url)
    fapi._check_response_code(r, 201)
    print("Successfully pushed {0}/{1}".format(args.namespace, args.method))

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
    print("Successfully redacted workflow.")

@fiss_cmd
def flow_acl(args):
    """ Get Access Control List for a workflow """
    r = fapi.get_repository_method_acl(args.namespace, args.method,
                                       args.snapshot_id, args.api_url)
    fapi._check_response_code(r, 200)
    for d in r.json():
        user = d['user']
        role = d['role']
        print('{0}\t{1}'.format(user, role))

@fiss_cmd
def flow_set_acl(args):
    """ Assign an ACL role to a list of users for a worklow. """
    acl_updates = [{"user": user, "role": args.role} for user in args.users]

    snap_id = args.snapshot_id

    if not snap_id:
        # get the latest snapshot_id for this method from the methods repo
        r = fapi.list_repository_methods(args.api_url)
        fapi._check_response_code(r, 200)
        flow_versions = [m for m in r.json()
                         if m['name'] == args.method and m['namespace'] == args.namespace]
        if len(flow_versions) == 0:
            print("Error: no versions of {0}/{1} found".format(args.namespace, args.method))
            return 1
        latest_version = sorted(flow_versions, key=lambda m: m['snapshotId'])[-1]
        snap_id = latest_version['snapshotId']

    r = fapi.update_repository_method_acl(args.namespace, args.method,
                                          snap_id, acl_updates,
                                          args.api_url)
    fapi._check_response_code(r, 200)
    print("Updated permissions for {0}/{1}:{2}".format(args.namespace, args.method, snap_id))

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

    # Sort for easier viewing, ignore case
    return sorted(results, key=lambda s: s.lower())

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

    # Parse the JSON for the workspace + namespace
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
        print(r)

@fiss_cmd
def config_acl(args):
    """ Get Access Control List for a method configuration. """
    r = fapi.get_repository_config_acl(args.namespace, args.config,
                                       args.snapshot_id, args.api_url)
    fapi._check_response_code(r, 200)
    for d in r.json():
        user = d['user']
        role = d['role']
        print('{0}\t{1}'.format(user, role))
    #return ['{0}\t{1}'.format(entry['user'], entry['role']) for entry in r.json() ]

@fiss_cmd
def config_get(args):
    """ Retrieve a method config from a workspace, send stdout """
    r = fapi.get_workspace_config(args.project, args.workspace,  args.namespace, args.config, args.api_url)
    fapi._check_response_code(r, 200)
    return r.text

@fiss_cmd
def config_copy(args):
    """ Copy a method config from one workspace to another """
    copy = fapi.get_workspace_config(args.project, args.fromspace,
                            args.namespace, args.config, args.api_url)
    fapi._check_response_code(copy, 200)

    # If existing one already exists, delete first
    r = fapi.get_workspace_config(args.project, args.tospace,
                            args.namespace, args.config, args.api_url)
    if r.status_code == 200:
        r = fapi.delete_workspace_config(args.project, args.tospace,
                            args.namespace, args.config, args.api_url)
        fapi._check_response_code(r, 204)

    r = fapi.create_workspace_config(args.project, args.tospace,
                            copy.json(), args.api_url)
    fapi._check_response_code(r, 201)

    return 0

@fiss_cmd
def attr_get(args):
    '''Retrieve set of attribute name/value pairs from a workspace: if one or
    more entities are specified then the attributes will be retrieved from
    those entities, otherwise the attributes defined at the workspace scope
    will be retrieved.  Returns a dict of name/value pairs.'''

    attributes = attrdict('')
    if args.entity_type:
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
        print(u(header))

        for entity_dict in entities:
            name = entity_dict['name']
            etype = entity_dict['entityType']
            attrs = entity_dict['attributes']
            for attr in attr_list:
                # Get attribute value
                if attr == "participant_id" and args.entity_type == "sample":
                    value = attrs['participant']['entityName']
                else:
                    value = attrs.get(attr, "")

                # If it's a dict, we get the entity name from the "items" section
                # Otherwise it's a string (either empty or the value of the attribute)
                # so no modifications are needed
                if type(value) == dict:
                    value = ",".join([i['entityName'] for i in value['items']])

                attributes[name] = value
    else:
        # Otherwise get workspace-scoped attributes
        r = fapi.get_workspace(args.project, args.workspace, args.api_url)
        fapi._check_response_code(r, 200)

        attrs = r.json()['workspace']['attributes']
        for name in sorted(attrs.keys()):
            if not args.attributes or name in args.attributes:
                attributes[name] = "{0}".format(attrs[name])

    return attributes

@fiss_cmd
def attr_set(args):
    """ Set attributes on a workspace or entities """
    if not args.entity_type:
        # Update workspace attributes
        prompt = "Set {0}={1} in {2}/{3}?\n[Y\\n]: ".format(
            args.attribute, args.value, args.project, args.workspace
        )

        if not args.yes and not _confirm_prompt("", prompt):
            return 0

        update = fapi._attr_set(args.attribute, args.value)
        r = fapi.update_workspace_attributes(args.project, args.workspace,
                                        [update], api_root=args.api_url)
        r = fapi._check_response_code(r, 200)
    else:
        if not args.entity:
            print("Error: please provide an entity to run on")
            return 1

        prompt = "Set {0}={1} for {2}:{3} in {4}/{5}?\n[Y\\n]: ".format(
            args.attribute, args.value, args.entity_type, args.entity,
            args.project, args.workspace
        )

        if not args.yes and not _confirm_prompt("", prompt):
            return 0

        update = fapi._attr_set(args.attribute, args.value)
        r = fapi.update_entity(args.project, args.workspace, args.entity_type,
                               args.entity, [update], api_root=args.api_url)
        fapi._check_response_code(r, 200)

    return 0

@fiss_cmd
def attr_delete(args):
    """ Delete attributes on a workspace or entities """

    if not args.entity_type:
        message = "WARNING: this will delete the following attributes in "
        message += "{0}/{1}\n\t".format(args.project, args.workspace)
        message += "\n\t".join(args.attributes)

        if not args.yes and not _confirm_prompt(message):
            return 0

        updates = [fapi._attr_rem(a) for a in args.attributes]
        r = fapi.update_workspace_attributes(args.project, args.workspace,
                                             updates, api_root=args.api_url)
        fapi._check_response_code(r, 200)

    else:
        #TODO: Implement this for entIties
        # Since there is no delete entity endpoint, we have to to two operations to delete
        # and attribute for an entity. First we must retrieve the entity_ids,
        # and any foreign keys (e.g. participant_id for sample_id), and then
        # construct a loadfile that deletes those entities. FireCloud uses the
        # magic keyword "__DELETE__" to indicate that the attribute should
        # be deleted.

        # Get entities

        entities = _entity_paginator(args.project, args.workspace,
                                     args.entity_type,
                                     page_size=1000, filter_terms=None,
                                     sort_direction="asc",api_root=args.api_url)

        # Now filter to just the entities requested
        if args.entities:
            entities = [e for e in entities if e['name'] in args.entities]

        # Now construct a loadfile to delete these attributes
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
        message = "WARNING: this will delete these attributes:\n\n"
        message += ','.join(args.attributes) + "\n\n"
        if args.entities:
            message += 'on these {0}s:\n\n'.format(args.entity_type)
            message += ', '.join(args.entities)
        else:
            message += 'on all {0}s'.format(args.entity_type)
        message += "\n\nin workspace {0}/{1}\n".format(args.project, args.workspace)
        if not args.yes and not _confirm_prompt(message):
            return 0

        #TODO: reconcile with other batch updates
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
            r = fapi.upload_entities(args.project, args.workspace, this_data,
                                     args.api_url)
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
        eprint("Error: destination project and namespace must differ from"
               " source workspace")
        return 1

    # First get the workspace attributes of the source workspace
    r = fapi.get_workspace(args.project, args.workspace, args.api_url)
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
                                    updates, api_root=args.api_url)
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

        message = "WARNING: This will insert null sentinels for "
        message += "these attributes:\n" + updates_table
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
            r = fapi.upload_entities(args.project, args.workspace, this_data,
                                     args.api_url)
            fapi._check_response_code(r, 200)

        return 0
    else:
        # TODO: set workspace attributes
        print("attr_fill_null requires an entity type")
        return 1

@fiss_cmd
def ping(args):
    """ Ping FireCloud Server """
    r = fapi.ping(args.api_url)
    fapi._check_response_code(r, 200)
    return r.content

@fiss_cmd
def mop(args):
    """ Clean up unreferenced data in a workspace """
    # First retrieve the workspace to get the bucket information
    if args.verbose:
        print("Retrieving workspace information...")
    r = fapi.get_workspace(args.project, args.workspace, args.api_url)
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
        print("Error retrieving files from bucket: " + e)
        return 1

    bucket_files = set(bucket_files.strip().split('\n'))
    if args.verbose:
        num = len(bucket_files)
        print("Found {0} files in bucket {1}".format(num, bucket))

    # Now build a set of files that are referenced in the bucket
    # 1. Get a list of the entity types in the workspace
    r = fapi.list_entity_types(args.project, args.workspace,
                              args.api_url)
    fapi._check_response_code(r, 200)
    entity_types = r.json().keys()

    # 2. For each entity type, request all the entities
    for etype in entity_types:
        if args.verbose:
            print("Getting annotations for " + etype + " entities...")
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
        print("No files to mop in " + workspace['workspace']['name'])
        return

    if args.verbose or args.dry_run:
        print("Found {0} files to delete:\n".format(len(deleteable_files))
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
        print("Deleting files with gsutil...")
    gsrm_proc = subprocess.Popen(gsrm_args, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    # Pipe the deleteable_files into gsutil
    result = gsrm_proc.communicate(input='\n'.join(deleteable_files))[0]
    if args.verbose:
        print(result.rstrip())

@fiss_cmd
def noop(args):
    if args.verbose:
        proj  = args.proj if args.proj else "unspecified"
        space = args.space if args.space else "unspecified"
        print('fiss no-op command: Project=%s, Space=%s' % (proj, space))

def config(*names):
    values = attrdict()
    if not names:
        names = fcconfig.keys()
    for key in names:
        values[key] = fcconfig.get(key, "__undefined__")
    return values

@fiss_cmd
def config_cmd(args):
    return config(*args.variable)

@fiss_cmd
def flow_start(args):
    '''Start running a workflow, on given entity in given space'''
    print("Starting {0} on {1} in {2}/{3}".format(
        args.config, args.entity, args.project, args.workspace
    ))
    r = fapi.create_submission(args.project, args.workspace,
                               args.namespace, args.config,
                               args.entity, args.entity_type, args.expression,
                               use_callcache=args.cache,
                               api_root=args.api_url)
    fapi._check_response_code(r, 201)
    id = r.json()['submissionId']
    print("Started {0}: id={1}".format(args.config, id))

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
        print('\n' + args.action + " " + sset + ":")

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
    print(r.content)
    print(len(r.json()))

@fiss_cmd
def supervise(args):
    """ Run Firehose-style workflow of workflows """
    project = args.project
    workspace = args.workspace
    namespace = args.namespace
    workflow = args.workflow
    sample_sets = args.sample_sets
    api_url = args.api_url
    recovery_file = args.json_checkpoint

    # If no somple sets are provided, run on all sample sets
    if not sample_sets:
        r = fapi.get_entities(args.project, args.workspace,
                              "sample_set", args.api_url)
        fapi._check_response_code(r, 200)
        sample_sets = [s['name'] for s in r.json()]

    message = "Sample Sets:\n\t".format(len(sample_sets))
    message += "\n\t".join(sample_sets)

    prompt = "\nLaunch workflow in " + project + "/" + workspace
    prompt += " on these sample sets? [Y\\n]: "


    if not args.yes and not _confirm_prompt(message, prompt):
        #Don't do it!
        return

    return supervisor.supervise(project, workspace, namespace,
                                workflow, sample_sets,
                                recovery_file, api_url)

@fiss_cmd
def supervise_recover(args):
    recovery_file = args.recovery_file
    return supervisor.recover_and_supervise(recovery_file)

@fiss_cmd
def space_search(args):
    """ Search for workspaces matching certain criteria """
    r = fapi.list_workspaces(args.api_url)
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
    pretty_spaces = sorted(pretty_spaces, key=lambda s: s.lower())
    return pretty_spaces

@fiss_cmd
def entity_copy(args):
    """ Copy entities from one workspace to another. """
    if not args.to_workspace:
        args.to_workspace = args.workspace
    if not args.to_project:
        args.to_project = args.project
    if (args.project == args.to_project
        and args.workspace == args.to_workspace):
        eprint("Error: destination project and namespace must differ from"
               " source workspace")
        return 1

    if not args.entities:
        # get a list of entities from source workspace matching entity_type
        ents = _entity_paginator(args.project, args.workspace, args.entity_type,
                                 page_size=500, filter_terms=None,
                                 sort_direction='asc', api_root=args.api_url)
        args.entities = [e['name'] for e in ents]

    prompt = "Copy {0} {1}(s) from {2}/{3} to {4}/{5}?\n[Y\\n]: "
    prompt = prompt.format(len(args.entities), args.entity_type, args.project,
                           args.workspace, args.to_project, args.to_workspace)

    if not args.yes and not _confirm_prompt("", prompt):
        return

    r = fapi.copy_entities(
        args.project, args.workspace, args.to_project, args.to_workspace,
        args.entity_type, args.entities, args.api_url
    )
    fapi._check_response_code(r, 201)

@fiss_cmd
def proj_list(args):
    """ List available billing projects """
    r = fapi.list_billing_projects(args.api_url)
    fapi._check_response_code(r, 200)
    projects = sorted(r.json(), key=lambda d: d['projectName'])
    print("Project\tRole")
    for p in projects:
        print(p['projectName'] + '\t' + p['role'])

@fiss_cmd
def config_validate(args):
    """Validate a workspace configuration. If provided an entity, also validate that
    the entity has the necessary attributes.
    """
    r = fapi.validate_config(args.project, args.workspace, args.namespace,
                             args.config, args.api_url)
    fapi._check_response_code(r, 200)
    entity_d = None
    config_d = r.json()
    if args.entity:
        entity_type = config_d['methodConfiguration']['rootEntityType']
        entity_r = fapi.get_entity(args.project, args.workspace,
                                   entity_type, args.entity, args.api_url)
        fapi._check_response_code(entity_r, [200,404])
        if entity_r.status_code == 404:
            print("Error: No {0} named '{1}'".format(entity_type, args.entity))
            return 2
        else:
            entity_d = entity_r.json()

    # also get the workspace info
    w = fapi.get_workspace(args.project, args.workspace, args.api_url)
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
    w = fapi.get_workspace(args.project, args.workspace, args.api_url)
    fapi._check_response_code(w, 200)
    workspace_d = w.json()

    if args.config and args.namespace and not args.entity:
        # See what entities I can run on with this config
        r = fapi.validate_config(args.project, args.workspace, args.namespace,
                                 args.config, args.api_url)
        fapi._check_response_code(r, 200)
        config_d = r.json()



        # First validate without any sample sets
        errs = sum(_validate_helper(args, config_d, workspace_d, None), [])
        if errs:
            print("Configuration contains invalid expressions")
            return 1

        # Now get  all the possible entities, and evaluate each
        entity_type = config_d['methodConfiguration']['rootEntityType']
        ent_r = fapi.get_entities(args.project, args.workspace, entity_type, args.api_url)
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
                                   args.entity_type, args.entity, args.api_url)
        fapi._check_response_code(entity_r, [200,404])
        if entity_r.status_code == 404:
            print("Error: No {0} named '{1}'".format(args.entity_type, args.entity))
            return 2
        entity_d = entity_r.json()

        # Now get all the method configs in the workspace
        conf_r = fapi.list_workspace_configs(args.project, args.workspace, args.api_url)
        fapi._check_response_code(conf_r, 200)

        # Iterate over configs in the workspace, and validate against them
        for cfg in conf_r.json():
            # If we limit search to a particular namespace, skip ones that don't match
            if args.namespace and cfg['namespace'] != args.namespace:
                continue

            # But we have to get the full description
            r = fapi.validate_config(args.project, args.workspace,
                                    cfg['namespace'], cfg['name'], args.api_url)
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
        ent_r = fapi.get_entities(args.project, args.workspace, args.entity_type, args.api_url)
        fapi._check_response_code(ent_r, 200)
        entities = ent_r.json()
        entity_names = sorted(e['name'] for e in entities)

        conf_r = fapi.list_workspace_configs(args.project, args.workspace, args.api_url)
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
                                    cfg['namespace'], cfg['name'], args.api_url)
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

def _entity_paginator(namespace, workspace, etype, page_size=500,
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
    print(*args, file=sys.stderr, **kwargs)

def __cmd_to_func(cmd):
    """ Returns the function object in this module matching cmd. """
    fiss_module = sys.modules[__name__]
    # Returns None if string is not a recognized FISS command
    func = getattr(fiss_module, cmd, None)
    if func and not hasattr(func, 'fiss_cmd'):
        func = None
    return func

def _valid_headerline(l):
    """return true if the given string is a valid loadfile header"""

    if not l:
        return False
    headers = l.split('\t')
    first_col = headers[0]

    tsplit = first_col.split(':')
    if len(tsplit) != 2:
        return False

    if tsplit[0] == 'entity':
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

def _batch_load(project, workspace, headerline, entity_data,
                chunk_size=500, api_url=fapi.PROD_API_ROOT, verbose=False):
    """ Submit a large number of entity updates in batches of chunk_size """
    if verbose:
        print("Batching " + str(len(entity_data)) + " updates to Firecloud...")

    #Parse the entity type from the first cell, e.g. "entity:sample_id"
    # First check that the header is valid
    if not _valid_headerline(headerline):
        print("Invalid loadfile header:\n" + headerline)
        return 1

    update_type = "membership" if headerline.startswith("membership") else "entitie"
    etype = headerline.split('\t')[0].split(':')[1].replace("_id", "")

    # Split entity_data into chunks
    total = int(len(entity_data) / chunk_size) + 1
    batch = 0
    for i in range(0, len(entity_data), chunk_size):
        batch += 1
        if verbose:
            print("Updating {0} {1}s {2}-{3}, batch {4}/{5}".format(
                etype, update_type, i+1, min(i+chunk_size, len(entity_data)), batch, total
            ))
        this_data = headerline + '\n' + '\n'.join(entity_data[i:i+chunk_size])

        # Now push the entity data to firecloud
        r = fapi.upload_entities(project, workspace, this_data, api_url)
        fapi._check_response_code(r, 200)

    return 0

__PatternsToFilter = [
    # This provides a systematic way of turning complex FireCloud messages
    # into a more comprehensible (easier to read) form: entries have the form
    #  [regex_to_match, replacement_template, match_groups_to_fill_in_template]
    ['^(.+)SlickWorkspaceContext\(Workspace\(([^,]+),([^,]*).*$', '%s%s::%s', (1,2,3) ],
]
for i in range(len(__PatternsToFilter)):
    __PatternsToFilter[i][0] = re.compile(__PatternsToFilter[i][0])

def __pretty_print_fc_exception(e):
    code = e.code
    e = json.loads(e.message)
    source = e["source"]
    msg = e["message"]
    for pattern in __PatternsToFilter:
        match = pattern[0].match(msg)
        if match:
            msg = pattern[1] % (match.group(*(pattern[2])))
            break
    print("Error %d (%s): %s" % (code, source, msg))

def unroll_value(value):
    retval = value if isinstance(value, int) else 0
    if isinstance(value, dict):
        for k, v in sorted(value.items()):
            print(u("{0}\t{1}".format(k,v)))
    elif isinstance(value, list):
        list(map(print, value))
    elif not isinstance(value, int):
        print(u("{0}".format(value)))
    return retval

#################################################
# Main, entrypoint for fissfc
################################################

def main(argv=None):

    if not argv:
        argv = sys.argv

    proj_required = not bool(fcconfig.project)
    meth_ns_required = not bool(fcconfig.method_ns)
    workspace_required = not bool(fcconfig.workspace)

    # Initialize core parser (TODO: Add longer description)
    descrip  = 'fissfc [OPTIONS] CMD [arg ...]\n'
    descrip += '       fissfc [ --help | -v | --version ]'
    parser = argparse.ArgumentParser(description='FISS: The FireCloud CLI')

    # Core Flags
    url_help = 'Firecloud api url. Your default is ' + fcconfig.api_url
    parser.add_argument('-u', '--url', dest='api_url',
                        default=fcconfig.api_url,
                        help=url_help)

    parser.add_argument("-v", "--version",
                action='version', version=__version__)

    parser.add_argument('-V', '--verbose', action='store_true',
                help='Turn on verbosity (e.g. show URL of REST calls)')

    parser.add_argument("-y", "--yes", action='store_true',
                help="Assume yes for any prompts")

    # Many commands share arguments, and we can make parent parsers to make it
    # easier to reuse arguments. Commands that operate on workspaces
    # all take a (google) project and a workspace name

    workspace_parent = argparse.ArgumentParser(add_help=False)
    workspace_parent.add_argument('-w', '--workspace', help='Workspace name',
                    default=fcconfig.workspace, required=workspace_required)

    proj_help =  'Project (workspace namespace). Required '
    proj_help += 'if no DEFAULT_PROJECT has been configured'
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
    acl_parent.add_argument('--users', help='FireCloud usernames. Use "public" to set global permissions.', nargs='+',
                            required=True)

    # Commands that operates on entity_types
    etype_parent = argparse.ArgumentParser(add_help=False)
    etype_parent.add_argument('-t', '--entity-type', required=True,
                             help="FireCloud entity type")

    # Commands that require an entity name
    entity_parent = argparse.ArgumentParser(add_help=False)
    entity_parent.add_argument(
        '-e', '--entity', required=True,
        choices=[
            'participant', 'participant_set', 'sample', 'sample_set',
            'pair', 'pair_set'
        ], help="FireCloud entity name"
    )

    # Commands that work with methods
    meth_parent = argparse.ArgumentParser(add_help=False)
    meth_parent.add_argument('-m', '--method', required=True,
                             help='Workflow/Method name')
    meth_parent.add_argument('-n', '--namespace', help='Method namespace',
                    default=fcconfig.method_ns, required=meth_ns_required)

    # Commands that work with method configurations
    conf_parent = argparse.ArgumentParser(add_help=False)
    conf_parent.add_argument('-c', '--config', required=True,
                             help='Method config name')
    conf_parent.add_argument('-n', '--namespace',
                help='Method config namespace',
                default=fcconfig.method_ns, required=meth_ns_required)

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
    subp = subparsers.add_parser('space_new', parents=[workspace_parent],
                                        description='Create new workspace')
    phelp = 'Limit access to the workspace to a specific authorization domain. '
    phelp += 'For dbGaP-controlled access (domain name: dbGapAuthorizedUsers) you must have linked NIH credentials to your account.'
    subp.add_argument('--authdomain', default="", help=phelp)
    subp.set_defaults(func=space_new)

    # Determine existence of workspace
    subp = subparsers.add_parser('space_exists', parents=[workspace_parent],
        description='Determine if given workspace exists in given project')
    phelp = 'Do not print message, only return numeric status'
    subp.add_argument('-q', '--quiet', action='store_true', help=phelp)
    subp.set_defaults(func=space_exists)

    #Delete workspace
    subp = subparsers.add_parser('space_delete', description='Delete workspace')
    subp.add_argument('-w', '--workspace', help='Workspace name', required=True)
    proj_help =  'Project (workspace namespace). Required '
    proj_help += 'if no DEFAULT_PROJECT has been configured'
    subp.add_argument('-p', '--project', default=fcconfig.project,
                help=proj_help, required=proj_required)
    subp.set_defaults(func=space_delete)

    # Get workspace information
    subp = subparsers.add_parser(
        'space_info', description='Show workspace information',
        parents=[workspace_parent]
    )
    subp.set_defaults(func=space_info)

    # List workspaces
    subp = subparsers.add_parser('space_list',
            description=
            'List available workspaces in projects (namespaces) to which you '\
            'have access. If you have a config file which defines a default '\
            'project, then only the workspaces in that project will be listed.'
    )
    subp.add_argument('-p', '--project', default=fcconfig.project,
            help='List spaces for projects whose names start with this prefix.'\
            ' You may also specify . (a dot), to list everything.')
    subp.set_defaults(func=space_list)

    # Lock workspace
    subp = subparsers.add_parser(
        'space_lock', description='Lock a workspace',
        parents=[workspace_parent]
    )
    subp.set_defaults(func=space_lock)

    # Unlock Workspace
    space_unlock_parser = subparsers.add_parser(
        'space_unlock', description='Unlock a workspace',
        parents=[workspace_parent]
    )
    space_unlock_parser.set_defaults(func=space_unlock)

    # Clone workspace
    clone_desc = 'Clone a workspace. The destination namespace or name must be '
    clone_desc += 'different from the workspace being cloned'
    subp = subparsers.add_parser(
        'space_clone', description=clone_desc,
        parents=[workspace_parent, dest_space_parent]
    )
    subp.set_defaults(func=space_clone)

    # Import data into a workspace
    subp = subparsers.add_parser(
        'entity_import', description='Import data into a workspace',
        parents=[workspace_parent]
    )
    subp.add_argument('-f','--tsvfile', required=True,
                               help='Tab-delimited loadfile')
    subp.add_argument('-C', '--chunk-size', default=500, type=int,
                               help='Maximum entities to import per api call')
    subp.set_defaults(func=entity_import)

    # List of entity types in a workspace
    subp = subparsers.add_parser(
        'entity_types', parents=[workspace_parent],
        description='List entity types in a workspace'
    )
    subp.set_defaults(func=entity_types)

    # List of entities in a workspace
    subp = subparsers.add_parser(
        'entity_list', description='List entity types in a workspace',
        parents=[workspace_parent]
    )
    subp.set_defaults(func=entity_list)

    # List of participants
    subp = subparsers.add_parser(
        'participant_list', description='List participants in a workspace',
        parents=[workspace_parent]
    )
    subp.set_defaults(func=participant_list)

    # List of samples
    subp = subparsers.add_parser(
        'sample_list', description='List samples in a workspace',
        parents=[workspace_parent]
    )
    subp.set_defaults(func=sample_list)

    # List of sample sets
    subp = subparsers.add_parser(
        'sset_list', description='List sample sets in a workspace',
        parents=[workspace_parent]
    )
    subp.set_defaults(func=sset_list)

    # Delete entity in a workspace
    subp = subparsers.add_parser(
        'entity_delete', description='Delete entity in a workspace',
        parents=[workspace_parent, etype_parent, entity_parent]
    )
    subp.set_defaults(func=entity_delete)

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
        parents=[meth_parent, acl_parent]
    )
    macl_parser.add_argument('-i', '--snapshot-id',
                             help="Snapshot ID (version) of method/config")
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
    cfg_list_parser.add_argument('-w', '--workspace', help='Workspace name',
                default=fcconfig.workspace, required=workspace_required)
    proj_help =  'Project (workspace namespace).'
    cfg_list_parser.add_argument('-p', '--project', default=fcconfig.project,
                                 help=proj_help, required=proj_required)
    cfg_list_parser.set_defaults(func=config_list)

    subp = subparsers.add_parser(
        'config_get', description='Retrieve method configuration (definition)',
        parents=[conf_parent]
    )
    subp.add_argument('-w', '--workspace', help='Workspace name',
                    default=fcconfig.workspace, required=workspace_required)
    subp.add_argument('-p', '--project', default=fcconfig.project,
                                 help='Project (workspace namespace)',
                                 required=proj_required)
    subp.set_defaults(func=config_get)

    subp = subparsers.add_parser('config_copy',
            description='Copy a method config from one workspace to another',
            parents=[conf_parent]
    )
    subp.add_argument('-p', '--project', default=fcconfig.project,
                                 help='Project (workspace namespace)',
                                 required=proj_required)
    subp.add_argument('-f', '--fromspace', help='from workspace',
                    default=fcconfig.workspace, required=workspace_required)
    subp.add_argument('-t', '--tospace', help='to workspace', required=True)
    subp.set_defaults(func=config_copy)

    # FIXME: continue subp = ... meme below, instead of uniquely naming each
    #        subparse; better yet, most of this can be greatly collapsed and
    #        pushed into a separate function and/or auto-generated

    # Config ACLs
    cfgacl_parser = subparsers.add_parser(
        'config_acl', description='Show users and roles for a configuration',
        parents=[conf_parent, snapshot_parent]
    )
    cfgacl_parser.set_defaults(func=config_acl)

    # Set ACL
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

    # Set attribute on workspace or entities
    attr_set_prsr = subparsers.add_parser(
        'attr_set', description="Set attributes on a workspace",
        parents=[workspace_parent]
    )
    attr_set_prsr.add_argument('-a', '--attribute', required=True,
                               metavar='attr', help='Name of attribute to set')
    attr_set_prsr.add_argument('-v', '--value', required=True,
                              help='Attribute value')
    attr_set_prsr.add_argument(
        '-t', '--entity-type', help=etype_help,
        choices=[
            'participant', 'participant_set', 'sample', 'sample_set',
            'pair', 'pair_set'
        ]
    )
    attr_set_prsr.add_argument('-e', '--entity', help="Entity to set attribute on")

    attr_set_prsr.set_defaults(func=attr_set)

    # Copy workspace attributes
    attr_copy_prsr = subparsers.add_parser(
        'attr_copy', description="Copy workspace attributes between workspaces",
        parents=[workspace_parent, dest_space_parent, attr_parent]
    )
    attr_copy_prsr.set_defaults(func=attr_copy)

    # delete attributes
    attr_del_prsr = subparsers.add_parser(
        'attr_delete', description="Delete attributes in a workspace",
        parents=[workspace_parent, attr_parent]
    )
    attr_del_prsr.add_argument(
        '-t', '--entity-type', choices=[
            'participant', 'participant_set', 'sample', 'sample_set',
            'pair', 'pair_set'
        ], help="FireCloud entity type"
    )
    attr_del_prsr.add_argument('-e', '--entities', nargs='*',
                               help='FireCloud entities')
    attr_del_prsr.set_defaults(func=attr_delete)

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

    subp = subparsers.add_parser('noop',
        description='Simple no-op command, for exercising interface')
    subp.set_defaults(func=noop, proj=fcconfig.project, space=fcconfig.workspace)

    subp = subparsers.add_parser('config',
        description='Display value(s) of one or more configuration variables')
    subp.add_argument('variable', nargs='*',
        help='Name of configuration variable (e.g. workspace, project)')
    subp.set_defaults(func=config_cmd)

    # Submit a workflow
    subp = subparsers.add_parser('flow_start',
        description='Start running workflow in a given space',
        parents=[workspace_parent, conf_parent, entity_parent]
    )
    #Duplicate entity type here since we want sample_set to be default
    etype_help =  'Entity type to assign null values, if attribute is missing.'
    etype_help += '\nDefault: sample_set'
    subp.add_argument(
        '-t', '--entity-type', help=etype_help,
        default='sample_set',
        choices=[
            'participant', 'participant_set', 'sample', 'sample_set',
            'pair', 'pair_set'
        ]
    )
    expr_help = "(optional) Entity expression to use when entity type doesn't"
    expr_help += " match the method configuration. Example: 'this.samples'"
    subp.add_argument('-x', '--expression', help=expr_help, default='')
    subp.add_argument('-C', '--cache', default=True,
        help='use previously cached results, if possible [%(default)s]')
    subp.set_defaults(func=flow_start)

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


    # Supervisor mode
    sup_help = "Run a Firehose-style workflow of workflows specified in DOT"
    sup_parser = subparsers.add_parser(
        'supervise', description=sup_help,
        parents=[workspace_parent]
    )
    sup_parser.add_argument('workflow', help='Workflow description in DOT')
    sup_parser.add_argument('-n', '--namespace', required=True,
                             help='Methods namespace')
    sup_parser.add_argument('-s', '--sample-sets', nargs='+',
                            help='Sample sets to run workflow on')
    jhelp = "File to save monitor data. This file can be passed to "
    jhelp = "fissfc supervise_recover in case the supervisor crashes"
    recovery = os.path.expanduser('~/.fiss/monitor_data.json')
    sup_parser.add_argument('-j', '--json-checkpoint', default=recovery,
                            help='Name of file to save monitor data')
    sup_parser.set_defaults(func=supervise)

    # Recover an old supervisor
    rec_help = "Recover a supervisor submission from the checkpoint file"
    rec_parser = subparsers.add_parser(
        'supervise_recover', description=rec_help
    )
    rec_parser.add_argument('recovery_file', default=recovery, nargs='?',
                            help='File where supervisor metadata was stored')
    rec_parser.set_defaults(func=supervise_recover)

    # Space search
    ssearch_prsr = subparsers.add_parser(
        'space_search', description="Search for workspaces"
    )
    ssearch_prsr.add_argument('-b', '--bucket', help='Regex to match bucketName')
    ssearch_prsr.set_defaults(func=space_search)


    ecopy_prsr = subparsers.add_parser(
        'entity_copy', description='Copy entities from one workspace to another',
        parents=[workspace_parent, dest_space_parent, etype_parent]
    )

    ecopy_prsr.add_argument(
        '-e', '--entities', nargs='+',
        help='Entities to copy. If omitted, all entities will be copied.'
    )
    ecopy_prsr.set_defaults(func=entity_copy)


    # List billing projects
    proj_list_prsr = subparsers.add_parser(
        'proj_list', description="List available billing projects"
    )
    proj_list_prsr.set_defaults(func=proj_list)

    #Validate config
    conf_val_prsr = subparsers.add_parser(
        'config_validate', description="Validate a workspace configuration",
        parents=[workspace_parent]
    )
    conf_val_prsr.add_argument(
        '-e', '--entity',
        help="Validate config against this entity. Entity is assumed to be the same type as the config's root entity type",
    )
    conf_val_prsr.add_argument('-c', '--config',
                               help='Method configuration name')
    conf_val_prsr.add_argument('-n', '--namespace',
                               help='Method configuration namespace')
    conf_val_prsr.set_defaults(func=config_validate)

    runnable_prsr = subparsers.add_parser(
        'runnable', description="Show me what configurations can be run on which sample sets.",
        parents=[workspace_parent]
    )
    runnable_prsr.add_argument('-c', '--config',
                               help='Method configuration name')
    runnable_prsr.add_argument('-n', '--namespace',
                               help='Method configuration namespace')
    runnable_prsr.add_argument(
       '-e', '--entity',
       help="Show me what configurations can be run on this entity",
    )
    runnable_prsr.add_argument(
        '-t', '--entity-type',
        choices=[
            'participant', 'participant_set', 'sample', 'sample_set',
            'pair', 'pair_set'
        ],
         help="FireCloud entity type"
    )
    runnable_prsr.set_defaults(func=runnable)

    # Create the .fiss directory if it doesn't exist
    fiss_home = os.path.expanduser("~/.fiss")
    if not os.path.isdir(fiss_home):
        os.makedirs(fiss_home)

    # Special cases, print help with no arguments
    if len(argv) == 1:
            parser.print_help()
    elif argv[1]=='-l':
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
        for c in sorted(choices):
            if search in c:
                print(u('\t{0}'.format(c)))
    elif argv[1] == '-F':
        # Show source for remaining args
        for fname in argv[2:]:
            # Get module name
            fiss_module = sys.modules[__name__]
            try:
                func = getattr(fiss_module, fname)
                source_lines = ''.join(getsourcelines(func)[0])
                print(u(source_lines))
            except AttributeError:
                pass
    else:
        # Otherwise parse args & call correct subcommand (skipping argv[0])
        args = parser.parse_args(argv[1:])
        if args.verbose:
            fapi.set_verbosity(1)

        try:
            result = args.func(args)
            if result == None:
                result = 0
        except FireCloudServerError as e:
            result = e.code
            if args.verbose:
                print(e.message),
            __pretty_print_fc_exception(e)

        return unroll_value(result)

if __name__ == '__main__':
    sys.exit(main())
