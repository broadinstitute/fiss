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
import re

from six import print_, iteritems, string_types, itervalues
from six.moves import input
from yapsy.PluginManager import PluginManager

from firecloud import api as fapi
from firecloud.errors import *
from firecloud.__about__ import __version__
from firecloud import supervisor

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
    project = args.project
    workspace = args.workspace
    chunk_size = args.chunk_size
    api_url = args.api_url
    verbose = args.verbose

    with open(args.tsvfile) as tsvf:
        headerline = tsvf.readline().strip()
        entity_data = [l.strip() for l in tsvf]

    if not _batch_load(project, workspace, headerline, entity_data,
                    chunk_size, api_url, verbose):

        print_('Successfully uploaded entities')
    else:
        print_('Error encountered trying to upload entities, quitting....')
        return 1


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
                ##Get attribute value
                if attr == "participant_id" and args.entity_type == "sample":
                    value = attrs['participant']['entityName']
                else:
                    value = attrs.get(attr, "")

                # If it's a dict, we get the entity name from the "items" section
                # Otherwise it's a string (either empty or the value of the attribute)
                # so no modifications are needed
                if type(value) == dict:
                    value = ",".join([i['entityName'] for i in value['items']])
                line += "\t" + value
            print_(line)

    #Otherwise get workspace attributes
    else:
        r = fapi.get_workspace(args.project, args.workspace, args.api_url)
        fapi._check_response_code(r, 200)

        workspace_attrs = r.json()['workspace']['attributes']

        for k in sorted(workspace_attrs.keys()):
            if not args.attributes or k in args.attributes:
                print_(k + "\t" + workspace_attrs[k])


@fiss_cmd
def attr_set(args):
    """ Set attributes on a workspace or entities """
    if not args.entity_type:
        # Update workspace attributes
        prompt = "Set {0}={1} in {2}/{3}?\n[Y\\n]: ".format(
            args.attribute, args.value, args.project, args.workspace
        )

        if not args.yes and not _confirm_prompt("", prompt):
            return #Don't do it!

        update = fapi._attr_set(args.attribute, args.value)
        r = fapi.update_workspace_attributes(args.project, args.workspace,
                                        [update], api_root=args.api_url)
        r = fapi._check_response_code(r, 200)
    else:
        #TODO: Implement this for entities
        raise NotImplementedError("attr_set not implemented for entities")
    print_("Done.")


@fiss_cmd
def attr_delete(args):
    """ Delete attributes on a workspace or entities """

    if not args.entity_type:
        message = "WARNING: this will delete the following attributes in "
        message += "{0}/{1}\n\t".format(args.project, args.workspace)
        message += "\n\t".join(args.attributes)

        if not args.yes and not _confirm_prompt(message):
            return #Don't do it!

        updates = [fapi._attr_rem(a) for a in args.attributes]
        r = fapi.update_workspace_attributes(args.project, args.workspace,
                                             updates, api_root=args.api_url)
        fapi._check_response_code(r, 200)

    else:
        #TODO: Implement this for entties
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
            return #Don't do it!


        #TODO: reconcile with other batch updates
        # Chunk the entities into batches of 500, and upload to FC
        if args.verbose:
            print_("Batching " + str(len(entity_data)) + " updates to Firecloud...")
        chunk_len = 500
        total = int(len(entity_data) / chunk_len) + 1
        batch = 0
        for i in range(0, len(entity_data), chunk_len):
            batch += 1
            if args.verbose:
                print_("Updating samples {0}-{1}, batch {2}/{3}".format(
                    i+1, min(i+chunk_len, len(entity_data)), batch, total
                ))
            this_data = entity_header + '\n' + '\n'.join(entity_data[i:i+chunk_len])

            # Now push the entity data back to firecloud
            r = fapi.upload_entities(args.project, args.workspace, this_data,
                                     args.api_url)
            fapi._check_response_code(r, 200)

    print_("Done.")


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
        print_("No workspace attributes defined in {0}/{1}".format(
            args.project, args.workspace
        ))
        return

    message = "This will copy the following workspace attributes to {0}/{1}\n"
    message = message.format(args.to_project, args.to_workspace)
    for k, v in sorted(iteritems(workspace_attrs)):
        message += '\t{0}\t{1}\n'.format(k, v)

    if not args.yes and not _confirm_prompt(message):
        return

    # make the attributes into updates
    updates = [fapi._attr_set(k,v) for k,v in iteritems(workspace_attrs)]
    r = fapi.update_workspace_attributes(args.to_project, args.to_workspace,
                                    updates, api_root=args.api_url)
    fapi._check_response_code(r, 200)
    print_("Done.")



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

    #Parse the JSON for the workspace + namespace
    workspaces = r.json()

    # Now filter based on the search terms. Each search term is treated as
    # a regular expression
    extra_terms = []
    if args.bucket:
        workspaces = [w for w in workspaces
                      if re.search(args.bucket, w['workspace']['bucketName'])]
        extra_terms.append('bucket')

    # TODO: add more filter terms

    # If there was only one result, print it the simple way
    if len(workspaces) == 1:
        ws = workspaces[0]['workspace']['namespace']
        ns = workspaces[0]['workspace']['name']
        print_(ns + '/' + ws)
    elif len(workspaces)==0:
        print_("No workspaces found matching search criteria")
    else:
    # Print all the matching results
        print_('\t'.join(["Workspace"] + extra_terms))
        pretty_spaces = []
        for space in workspaces:
            ns = space['workspace']['namespace']
            ws = space['workspace']['name']
            pspace = ns + '/' + ws
            if args.bucket:
                b = space['workspace']['bucketName']
                pspace += '\t' + b

            pretty_spaces.append(pspace)


        #Sort for easier viewing, ignore case
        pretty_spaces = sorted(pretty_spaces, key=lambda s: s.lower())
        for s in pretty_spaces:
            print_(s)


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
    print_("Done.")

@fiss_cmd
def proj_list(args):
    """ List available billing projects """
    r = fapi.list_billing_projects(args.api_url)
    fapi._check_response_code(r, 200)
    projects = sorted(r.json(), key=lambda d: d['projectName'])
    print_("Project\tRole")
    for p in projects:
        print_(p['projectName'] + '\t' + p['role'])


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
    print_(*args, file=sys.stderr, **kwargs)


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
        print_("Batching " + str(len(entity_data)) + " updates to Firecloud...")

    #Parse the entity type from the first cell, e.g. "entity:sample_id"
    # First check that the header is valid
    if not _valid_headerline(headerline):
        print_("Invalid loadfile header:\n" + headerline)
        return 1

    update_type = "membership" if headerline.startswith("membership") else "entitie"
    etype = headerline.split('\t')[0].split(':')[1].replace("_id", "")

    # Split entity_data into chunks
    total = int(len(entity_data) / chunk_size) + 1
    batch = 0
    for i in range(0, len(entity_data), chunk_size):
        batch += 1
        if verbose:
            print_("Updating {0} {1}s {2}-{3}, batch {4}/{5}".format(
                etype, update_type, i+1, min(i+chunk_size, len(entity_data)), batch, total
            ))
        this_data = headerline + '\n' + '\n'.join(entity_data[i:i+chunk_size])

        # Now push the entity data to firecloud
        r = fapi.upload_entities(project, workspace, this_data, api_url)
        fapi._check_response_code(r, 200)

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
    parser = argparse.ArgumentParser(
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

    dest_space_parent = argparse.ArgumentParser(add_help=False)
    dest_space_parent.add_argument("-P", "--to-project",
                               help="Project (Namespace) of clone workspace")
    dest_space_parent.add_argument("-W", "--to-workspace",
                               help="Name of clone workspace")

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
        'space_clone', description=clone_desc,
        parents=[workspace_parent, dest_space_parent]
    )

    clone_parser.set_defaults(func=space_clone)

    #Import data into a workspace
    import_parser = subparsers.add_parser(
        'entity_import', description='Import data into a workspace',
        parents=[workspace_parent]
    )
    import_parser.add_argument('-f','--tsvfile', required=True,
                               help='Tab-delimited loadfile')
    import_parser.add_argument('-C', '--chunk-size', default=500, type=int,
                               help='Maximum entities to import per api call')
    import_parser.set_defaults(func=entity_import)

    # List of entity types in a workspace
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

    # List of participants
    pl_parser = subparsers.add_parser(
        'participant_list', description='List participants in a workspace',
        parents=[workspace_parent]
    )
    pl_parser.set_defaults(func=participant_list)

    # List of samples
    sl_parser = subparsers.add_parser(
        'sample_list', description='List samples in a workspace',
        parents=[workspace_parent]
    )
    sl_parser.set_defaults(func=sample_list)

    # List of sample sets
    ssetl_parser = subparsers.add_parser(
        'sset_list', description='List sample sets in a workspace',
        parents=[workspace_parent]
    )
    ssetl_parser.set_defaults(func=sset_list)

    # Delete entity in a workspace
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


    # Create the .fiss directory if it doesn't exist
    fiss_home = os.path.expanduser("~/.fiss")
    if not os.path.isdir(fiss_home):
        os.makedirs(fiss_home)

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
