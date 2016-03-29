#! /usr/bin/env python

"""
This module provides python bindings for the Firecloud API.
For more details see https://software.broadinstitute.org/firecloud/

To see how the python bindings map to the RESTful endpoints view 
the README at https://pypi.python.org/pypi/firecloud.
"""
import json
import httplib2
import urllib
from oauth2client.client import GoogleCredentials
from os.path import expanduser, isfile
from firecloud.errors import FireCloudServerError
import sys
from six import print_

PROD_API_ROOT = "https://api.firecloud.org/api"

#################################################
# Utilities
#################################################
def _credentials_exist():
    """Return true if google default credentials exist"""
    pth = expanduser('~/.config/gcloud/application_default_credentials.json')
    return isfile(pth)


def _gcloud_authorized_http():
    """Create and return a gcloud authorized Http object"""
    if not _credentials_exist():
        print_("ERROR: Could not find default google SDK credentials")
        print_("Ensure that the cloud SDK is installed, and then run")
        print_("    gcloud auth login")
        sys.exit(1)

    credentials = GoogleCredentials.get_application_default()
    http = httplib2.Http(".cache")
    return credentials.authorize(http)

def _check_response(response, content, expected):
    """Raise FireCloudServerError if response status is unexpected"""
    if response.status not in expected:
        raise FireCloudServerError(response.status, content)

#################################################
# API calls, see https://api.firecloud.org/
#################################################
def get_workspaces(api_root=PROD_API_ROOT):
    """Request list of FireCloud workspaces."""
    http = _gcloud_authorized_http()
    return http.request("{0}/workspaces".format(api_root))

def create_workspace(namespace, name, protected=False,
                     attributes=dict(), api_root=PROD_API_ROOT):
    """Create a new FireCloud Workspace.

    Args:
        namespace (str): Google project for the workspace
        name (str): Workspace name
        protected (bool): If True, this workspace is protected by dbGaP 
            credentials. This option is only available if your FireCloud 
            account is linked to your NIH account.
        attributes (dict): Workspace attributes as key value pairs
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    body_dict = { "namespace": namespace, 
                  "name": name,
                  "attributes": attributes,
                  "isProtected": protected }
    json_body = json.dumps(body_dict)

    return http.request("{0}/workspaces".format(api_root),
                        "POST", 
                        headers=headers,
                        body=json_body)

def delete_workspace(namespace, workspace,api_root=PROD_API_ROOT):
    """Delete FireCloud Workspace.

    Note: This action is not reversible. Be careful! 

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}".format(api_root, namespace, workspace)
    return http.request(uri, "DELETE")

def get_workspace(namespace, workspace,api_root=PROD_API_ROOT):
    """Request FireCloud Workspace information.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}".format(api_root, namespace, workspace)
    return http.request(uri)

def get_workspace_acl(namespace, workspace,api_root=PROD_API_ROOT):
    """Request FireCloud access aontrol list for workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/acl".format(api_root, namespace, workspace)
    return http.request(uri)

def update_workspace_acl(namespace, workspace, 
                         acl_updates, api_root=PROD_API_ROOT):
    """Update workspace access control list.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production
        acl_updates (list(dict)): Acl updates as dicts with two keys:
            "email" - Firecloud user email 
            "accessLevel" - one of "OWNER", "READER", "WRITER", "NO ACCESS"
            Example: {"email":"user1@mail.com", "accessLevel":"WRITER"}
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/acl".format(api_root, namespace, workspace)
    headers = {"Content-type":  "application/json"}
    json_body = json.dumps(acl_updates)
    return http.request(uri, "PATCH", headers=headers, body=json_body)

def clone_workspace(from_namespace, from_workspace, 
                    to_namespace, to_workspace, api_root=PROD_API_ROOT):
    """Clone a FireCloud workspace.

    A clone is a shallow copy of a FireCloud workspace, enabling
    easy sharing of data, such as TCGA data, without duplication.

    Args:
        from_namespace (str): Source workspace's google project (namespace)
        from_workspace (str): Source workspace's name
        to_namespace (str): Target workspace's google project
        to_workspace (str): Target workspace's name
        api_root(str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    body_dict = {"namespace": to_namespace, 
                 "name": to_workspace,
                 "attributes": dict()}
    json_body = json.dumps(body_dict)

    uri = "{0}/workspaces/{1}/{2}/clone".format(api_root,
                                               from_namespace, 
                                               from_workspace)
    return http.request(uri, "POST", headers=headers, body=json_body)

def lock_workspace(namespace, workspace, api_root=PROD_API_ROOT):
    """Lock FireCloud workspace, making it read-only.

    This prevents modifying attributes or submitting workflows
    in the workspace. Can be undone with unlock_workspace()

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production 
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/lock".format(api_root, namespace, workspace)
    return http.request(uri, "PUT")

def unlock_workspace(namespace, workspace, api_root=PROD_API_ROOT):
    """Unlock FireCloud workspace.

    Enables modifications to a workspace. See lock_workspace()

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production 
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/unlock".format(api_root, 
                                                 namespace, workspace)
    return http.request(uri, "PUT")


def get_configs(namespace, workspace, api_root=PROD_API_ROOT):
    """List method configurations in workspace.
    
    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production 
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/methodconfigs".format(api_root, 
                                                        namespace, workspace)
    return http.request(uri)

def create_config(namespace, workspace, mnamespace, method,
                  root_etype, api_root=PROD_API_ROOT):
    """Create method configuration in workspace.
    
    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        mnamespace (str): Method namespace
        method (str): Method name
        root_etype (str): Root entity type
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    body = { "namespace": mnamespace,
             "name": method,
             "rootEntityType": root_etype
           }
    body = json.dumps(body)
    uri = "{0}/workspaces/{1}/{2}/methodconfigs".format(api_root, 
                                                        namespace, workspace)
    return http.request(uri, "POST", headers=headers, body=body)

def delete_config(namespace, workspace, cnamespace, 
                  config, api_root=PROD_API_ROOT):
    """Delete method configuration in workspace.
    
    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        mnamespace (str): Method namespace
        method (str): Method name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/methodconfigs/{3}/{4}".format(
        api_root, namespace, workspace, cnamespace, config)
    return http.request(uri, "DELETE")

def get_config(namespace, workspace, cnamespace, 
               config, api_root=PROD_API_ROOT):
    """Get method configuration in workspace.
    
    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        cnamespace (str): Config namespace
        config (str): Config name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/methodconfigs/{3}/{4}".format(
        api_root, namespace, workspace, cnamespace, config)
    return http.request(uri)

def update_config(namespace, workspace, cnamespace, 
                  config, new_namespace, new_name,
                  root_etype, api_root=PROD_API_ROOT):
    """Update method configuration in workspace.
    
    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        cnamespace (str): Configuration namespace
        config (str): Configuration name
        new_namespace (str): Updated config namespace
        new_name (str): Updated config name
        root_etype (str): New root entity type
        api_root (str): FireCloud API url, if not production
    """

    headers = {"Content-type":  "application/json"}
    body = {
            "namespace": new_namespace,
            "name": new_name,
            "rootEntityType": root_etype 
            }
    body = json.dumps(body)
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/methodconfigs/{3}/{4}".format(
        api_root, namespace, workspace, cnamespace, config)
    return http.request(uri, "PUT", headers=headers, body=body)

def rename_config(namespace, workspace, cnamespace, 
                  config, new_namespace, new_name, api_root=PROD_API_ROOT):
    """Rename a method configuration in a workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        mnamespace (str): Config namespace
        config (str): Method name
        new_namespace (str): Updated method namespace
        new_name (str): Updated method name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    body = {
            "namespace": new_name,
            "name": new_namespace
           }
    body = json.dumps(body)
    uri = "{0}/workspaces/{1}/{2}/methodconfigs/{3}/{4}/rename".format(
        api_root, namespace, workspace, cnamespace, config)
    return http.request(uri, "POST", headers=headers, body=body)

def validate_config(namespace, workspace, cnamespace, 
                    config, api_root=PROD_API_ROOT):
    """Get syntax validation for a configuration.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        cnamespace (str): Configuration namespace
        config (str): Configuration name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/methodconfigs/{3}/{4}/validate".format(
        api_root, namespace, workspace, cnamespace, config)
    return http.request(uri)

def copy_config_from_repo(namespace, workspace, from_cnamespace, 
                          from_config, from_snapshot_id, to_cnamespace, 
                          to_config, api_root=PROD_API_ROOT):
    """Copy a method config from the methods repository to a workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        from_cnamespace (str): Source configuration namespace
        from_config (str): Source configuration name
        from_snapshot_id (int): Source configuration snapshot_id
        to_cnamespace (str): Target configuration namespace
        to_config (str): Target configuration name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    body = {
            "configurationNamespace": from_cnamespace,
            "configurationName": from_config,
            "configurationSnapshotId": from_snapshot_id,
            "destinationNamespace": to_cnamespace,
            "destinationName": to_config
           }
    body = json.dumps(body)
    uri = "{0}/workspaces/{1}/{2}/method_configs/copyFromMethodRepo".format(
        api_root, namespace, workspace, cnamespace, config)
    return http.request(uri, "POST", headers=headers, body=body)

def copy_config_to_repo():
    """Copy a method config from a workspace to the methods repository.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        from_cnamespace (str): Source configuration namespace
        from_config (str): Source configuration name
        to_cnamespace (str): Target configuration namespace
        to_config (str): Target configuration name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    body = {
            "configurationNamespace": to_cnamespace,
            "configurationName": to_config,
            "sourceNamespace": from_cnamespace,
            "sourceName": from_config
           }
    body = json.dumps(body)
    uri = "{0}/workspaces/{1}/{2}/method_configs/copyToMethodRepo".format(
        api_root, namespace, workspace, cnamespace, config)
    return http.request(uri, "POST", headers=headers, body=body)

def upload_entities_tsv(namespace, workspace, 
                        entities_tsv, api_root=PROD_API_ROOT):
    """Upload entities from a tsv loadfile.

    A loadfile is a tab-separated text file with a header row 
    describing entity type and attribute names, followed by
    rows of entities and their attribute values.

        Ex:
            entity:participant_id   age   alive
            participant_23           25       Y
            participant_27           35       N
    
    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        entities_tsv (file): Loadfile, see format above
        api_root (str): FireCloud API url, if not production 
    """
    http = _gcloud_authorized_http()
    with open(entities_tsv, "r") as tsv:
        entity_data = tsv.read()
    request_body = urllib.urlencode({"entities" : entity_data})
    headers = {'Content-type':  "application/x-www-form-urlencoded"}
    uri = "{0}/workspaces/{1}/{2}/importEntities".format(api_root, 
                                                         namespace, 
                                                         workspace)
    return http.request(uri, "POST", headers=headers, body=request_body)

def upload_entities(namespace, workspace, 
                    entity_data, api_root=PROD_API_ROOT):
    """Upload entities from string.

    Note: Equivalent to upload_entities_tsv(), except 
        entity_data is a string, not a file.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        entity_data (str): TSV string describing entites
        api_root (str): FireCloud API url, if not production 
    """

    http = _gcloud_authorized_http()
    request_body = urllib.urlencode({"entities" : entity_data})
    headers = {'Content-type':  "application/x-www-form-urlencoded"}
    uri = "{0}/workspaces/{1}/{2}/importEntities".format(api_root, 
                                                         namespace, 
                                                         workspace)
    return http.request(uri, "POST", headers=headers, body=request_body)


def get_submissions(namespace, workspace, api_root=PROD_API_ROOT):
    """List submissions in FireCloud workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production 
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/submissions".format(
        api_root, namespace, workspace)
    return http.request(uri)

def create_submission(wnamespace, workspace, cnamespace, config,
                      entity, etype, expression, api_root=PROD_API_ROOT):
    """Submit job in FireCloud workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        cnamespace (str): Method configuration namespace
        config (str): Method configuration name
        entity (str): Entity to submit job on. Should be the same type as
            the root entity type of the method config, unless an 
            expression is used
        expression (str): Instead of using entity as the root entity,
            evaluate the root entity from this expression.
        api_root (str): FireCloud API url, if not production 
    """

    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/submissions".format(
        api_root, wnamespace, workspace)
    body = { "methodConfigurationNamespace" : cnamespace,
             "methodConfigurationName" : config,
             "entityType" : etype,
             "entityName" : entity,
             "expression" : expression
            }

    body = json.dumps(body)
    headers = {"Content-type":  "application/json"}

    return http.request(uri, "POST", headers=headers, body=body)

def abort_sumbission(namespace, workspace, 
                     submission_id, api_root=PROD_API_ROOT):
    """Abort running job in a workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        api_root (str): FireCloud API url, if not production 
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}".format(
        api_root, namespace, workspace, submission_id)
    return http.request(uri, "DELETE")

def get_submission(namespace, workspace, 
                   submission_id, api_root=PROD_API_ROOT):
    """Request submission information.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        api_root (str): FireCloud API url, if not production 
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}".format(
        api_root, namespace, workspace, submission_id)
    return http.request(uri)

def get_workflow_outputs(namespace, workspace, 
                         submission_id, workflow_id, api_root=PROD_API_ROOT):
    """Request the outputs for a workflow in a submission.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        workflow_id (str): Workflow's unique identifier.
        api_root (str): FireCloud API url, if not production 
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/".format(api_root, namespace, workspace)
    uri += "submissions/{0}/workflows/{1}/outputs".format(
        submission_id, workflow_id)
    return http.request(uri) 

def get_entity_types(namespace, workspace, api_root=PROD_API_ROOT):
    """List the entity types present in a workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities".format(
        api_root, namespace, workspace)
    return http.request(uri)

def get_entities_with_type(namespace, workspace,
                           api_root=PROD_API_ROOT):
    """List entities in a workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities_with_type".format(
        api_root, namespace, workspace)
    return http.request(uri)

def get_entities(namespace, workspace, etype, api_root=PROD_API_ROOT):
    """List entities of given type in a workspace.

    Response content will be in JSON format.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        etype (str): Entity type
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}".format(
        api_root, namespace, workspace, etype)
    return http.request(uri)

def get_entities_tsv(namespace, workspace, etype, api_root=PROD_API_ROOT):
    """List entities of given type in a workspace as a TSV.

    Identical to get_entities(), but the response is a TSV.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        etype (str): Entity type
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/tsv".format(
        api_root, namespace, workspace, etype)
    return http.request(uri)

def get_entity(namespace, workspace, etype, ename, api_root=PROD_API_ROOT):
    """Request entity information.

    Gets entity metadata and attributes.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        etype (str): Entity type
        ename (str): The entity's unique id
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/{4}".format(
        api_root, namespace, workspace, etype, ename)
    return http.request(uri)

def delete_entity(namespace, workspace, etype, ename, api_root=PROD_API_ROOT):
    """Delete entity in a workspace.

    Note: This action is not reversible. Be careful! 

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        etype (str): Entity type
        ename (str): The entity's unique id
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/{4}".format(
        api_root, namespace, workspace, etype, ename)
    return http.request(uri, "DELETE")

def delete_participant(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete participant in a workspace.

    Note: This action is not reversible. Be careful! 

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        name (str): participant_id
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity(namespace, workspace, "participant",
                         name, api_root)

def delete_participant_set(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete participant set in a workspace.

    Note: This action is not reversible. Be careful! 

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        name (str): participant_set_id
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity(namespace, workspace, "participant_set", 
                         name, api_root)

def delete_sample(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete sample in a workspace.

    Note: This action is not reversible. Be careful! 

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        name (str): sample_id
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity(namespace, workspace, "sample", name, api_root)

def delete_sample_set(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete sample set in a workspace.

    Note: This action is not reversible. Be careful! 

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        name (str): sample_set_id
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity(namespace, workspace, "sample_set", name, api_root)

def delete_pair(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete pair in a workspace.

    Note: This action is not reversible. Be careful! 

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        name (str): pair_id
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity(namespace, workspace, "pair", name, api_root)

def delete_pair_set(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete pair set in a workspace.

    Note: This action is not reversible. Be careful! 

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        name (str): pair_set_id
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity(namespace, workspace, "pair_set", name, api_root)

def get_status(api_root=PROD_API_ROOT):
    """Request the status of FireCloud services."""
    http = _gcloud_authorized_http()
    uri = "{0}/status".format(api_root)
    return http.request(uri)

def ping(api_root=PROD_API_ROOT):
    """Ping API."""
    http = _gcloud_authorized_http()
    uri = "{0}/status/ping".format(api_root)
    return http.request(uri)

def get_repository_methods(api_root=PROD_API_ROOT):
    """List methods in the methods repository."""
    http = _gcloud_authorized_http()
    uri = "{0}/methods".format(api_root)
    return http.request(uri)

def get_repository_configs(api_root=PROD_API_ROOT):
    """List configurations in the methods repository."""
    http = _gcloud_authorized_http()
    uri = "{0}/configurations".format(api_root)
    return http.request(uri)

def get_config_template(namespace, method, version, api_root=PROD_API_ROOT):
    """Get the configuration template for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        version (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/template".format(api_root)
    headers = {"Content-type":  "application/json"}
    body_dict = {"methodNamespace": namespace, 
                 "methodName": method,
                 "methodVersion": version}
    json_body = json.dumps(body_dict)
    return http.request(uri, "POST", headers=headers, body=json_body)

def get_inputs_outputs(namespace, method, snapshot_id, api_root=PROD_API_ROOT):
    """Get a description of the inputs and outputs for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/inputsOutputs".format(api_root)
    headers = {"Content-type":  "application/json"}
    body_dict = {"methodNamespace": namespace, 
                 "methodName": method,
                 "methodVersion": snapshot_id}
    json_body = json.dumps(body_dict)
    return http.request(uri, "POST", headers=headers, body=json_body)

def get_repository_config(namespace, config,
                          snapshot_id, api_root=PROD_API_ROOT):
    """Get a method configuration from the methods repository.

    Args:
        namespace (str): Methods namespace
        config (str): config name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/configurations/{1}/{2}/{3}".format(
        api_root, namespace, config, snapshot_id)
    return http.request(uri)

def get_method(namespace, method, snapshot_id, api_root=PROD_API_ROOT):
    """Get a method definition from the method repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        version (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/methods/{1}/{2}/{3}".format(
        api_root, namespace, method, snapshot_id)
    return http.request(uri) 

def get_repository_method_acl(namespace, method, 
                              snapshot_id, api_root=PROD_API_ROOT):
    """Get permissions for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        version (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/methods/{1}/{2}/{3}/permissions".format(
        api_root, namespace, method, snapshot_id)
    return http.request(uri)

def update_repository_method_acl(namespace, method, snapshot_id,
                                 acl_updates, api_root=PROD_API_ROOT):
    """Set method permissions.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method
        acl_updates (list(dict)): List of access control updates
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    json_body = json.dumps(acl_updates)
    uri = "{0}/methods/{1}/{2}/{3}/permissions".format(
        api_root, namespace, method, snapshot_id)
    return http.request(uri, "POST", headers=headers, body=json_body)

def get_repository_config_acl(namespace, config, 
                              snapshot_id, api_root=PROD_API_ROOT):
    """Get configuration permissions.

    The configuration should exist in the methods repository.

    Args:
        namespace (str): Configuration namespace
        config (str): Configuration name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/configurations/{1}/{2}/{3}/permissions".format(
        api_root, namespace, config, snapshot_id)
    return http.request(uri)

def update_repository_config_acl(namespace, config, snapshot_id,
                                 acl_updates, api_root=PROD_API_ROOT):
    """Set configuration permissions.

    The configuration should exist in the methods repository.

    Args:
        namespace (str): Configuration namespace
        config (str): Configuration name
        snapshot_id (int): snapshot_id of the method
        acl_updates (list(dict)): List of access control updates
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    json_body = json.dumps(acl_updates)
    uri = "{0}/configurations/{1}/{2}/{3}/permissions".format(
        api_root, namespace, config, snapshot_id)
    return http.request(uri, "POST", headers=headers, body=json_body)

def update_workflow(namespace, method, synopsis,
                  wdl, doc=None, api_root=PROD_API_ROOT):
    """Create/Update workflow definition.

    FireCloud will create a new snapshot_id for the given workflow.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        synopsis (str): short (<80 char) description of method
        wdl (file): Workflow Description Language file
        doc (file): (Optional) Additional documentation
        api_root (str): FireCloud API url, if not production 

    """
    with open(wdl, 'r') as wf:
        wdl_payload = wf.read()
    if doc is not None:
        with open (doc, 'r') as df:
            doc = df.read()
    else:
        doc = ""

    add_dict = {"namespace": namespace,
                "name": method,
                "entityType": "Workflow",
                "payload": wdl_payload,
                "documentation": doc,
                "synopsis": synopsis
                }
    body = json.dumps(add_dict)
    headers = {"Content-type":  "application/json"}
    http = _gcloud_authorized_http()
    uri = "{0}/methods".format(api_root)

    return http.request(uri, "POST", headers=headers, body=body)

def delete_workflow(namespace, name, snapshot_id, api_root=PROD_API_ROOT):
    """Redact a version of a workflow.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/methods/{1}/{2}/{3}".format(api_root, namespace, 
                                           name, snapshot_id)
    return http.request(uri, "DELETE")

def update_workspace_attributes(namespace, workspace,
                                attrs, api_root=PROD_API_ROOT):
    """Update or remove workspace attributes.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production
        attrs (list(dict)): List of update operations for workspace attributes.
            Use the helper dictionary construction functions to create these:

            _attr_up()      : Set/Update attribute
            _attr_rem()     : Remove attribute
            _attr_ladd()    : Add list member to attribute
            _attr_lrem()    : Remove list member from attribute
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/updateAttributes".format(
        api_root, namespace, workspace)

    headers = {"Content-type": "application/json"}
    body = json.dumps(attrs)

    return http.request(uri, "PATCH", headers=headers, body=body)

# Helper functions to create attribute update dictionaries
def _attr_up(attr, value):
    """Create an 'update 'dictionary for update_workspace_attributes()"""
    return { "op"                 : "AddUpdateAttribute",
             "attributeName"      : attr,
             "addUpdateAttribute" : value
           }

def _attr_rem(attr):
    """Create a 'remove' dictionary for update_workspace_attributes()"""
    return { "op"             : "RemoveAttribute",
             "attributeName"  : attr
           }

def _attr_ladd(attr, value):
    """Create a 'list add' dictionary for update_workspace_attributes()"""
    return { "op"                 : "AddListMember",
             "attributeName"      : attr,
             "addUpdateAttribute" : value
           }

def _attr_lrem(attr, value):
    """Create a 'list remove' dictionary for update_workspace_attributes()"""
    return { "op"                 : "RemoveListMember",
             "attributeName"      : attr,
             "addUpdateAttribute" : value
           }

def copy_entities(from_namespace, from_workspace, to_namespace, 
                  to_workspace, etype, enames, api_root=PROD_API_ROOT):
    """Copy entities between workspaces

    Args:
        from_namespace (str): Source workspace's google project (namespace)
        from_workspace (str): Source workspace's name
        to_namespace (str): Target workspace's google project
        to_workspace (str): Target workspace's name
        etype (str): Entity type
        enames (list(str)): List of entity names to copy
        api_root(str): FireCloud API url, if not production
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities/copy".format(
        api_root, to_namespace, to_workspace)
    headers = {"Content-type":  "application/json"}
    body = { "sourceWorkspace": {
                "namespace": from_namespace,
                "name": from_workspace
             },
             "entityType": etype,
             "entityNames": enames
           } 
    body = json.dumps(body)
    return http.request(uri, "POST", headers=headers, body=body)  

def get_billing_projects(api_root=PROD_API_ROOT):
    """Get activation information for the logged-in user."""
    http = _gcloud_authorized_http()
    return http.request("{0}/profile/billing".format(api_root))
