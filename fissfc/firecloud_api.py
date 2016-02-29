#! /usr/bin/env python

"""
This module provides python bindings for the Firecloud API.
For more details see https://software.broadinstitute.org/firecloud/
"""

import json
import httplib2
import urllib
from oauth2client.client import GoogleCredentials


PROD_API_ROOT = "https://portal.firecloud.org/service/api"

def _gcloud_authorized_http():
    """
    Create and returna an gcloud authorized Http object
    """
    http = httplib2.Http(".cache")
    credentials = GoogleCredentials.get_application_default()
    return credentials.authorize(http)

#################################################
# Workspaces
#################################################

def list_workspaces(api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    return http.request("{0}/workspaces".format(api_root))

def create_workspace(namespace, workspace, attributes=dict(), api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    body_dict = {"namespace": namespace, 
                 "name": workspace,
                 "attributes": attributes}
    json_body = json.dumps(body_dict)

    return http.request("{0}/workspaces".format(api_root),
                        "POST", 
                        headers=headers,
                        body=json_body)

def delete_workspace(namespace, workspace,api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}".format(api_root, namespace, workspace)
    return http.request(uri, "DELETE")

def get_workspace(namespace, workspace,api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}".format(api_root, namespace, workspace)
    return http.request(uri)

def get_workspace_acl(namespace, workspace,api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/acl".format(api_root, namespace, workspace)
    return http.request(uri)

def update_workspace_acl(namespace, workspace, acl_updates, api_root=PROD_API_ROOT):
    """
    Update the workspace ACL. acl_updates should be a list of dictionaries,
    with two keys:
        "email" - whose value is a user email, e.g. "timdef@broadinstitute.org
        "accessLevel" - whose value is one of "OWNER", "READER", "WRITER", "NO ACCESS"
    """
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/acl".format(api_root, namespace, workspace)
    headers = {"Content-type":  "application/json"}
    json_body = json.dumps(acl_updates)
    return http.request(uri, "PATCH", headers=headers, body=json_body)

def clone_workspace(from_namespace, from_workspace, 
                    to_namespace, to_workspace, api_root=PROD_API_ROOT):
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
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/lock".format(api_root, namespace, workspace)
    return http.request(uri, "PUT")

def unlock_workspace(namespace, workspace, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/unlock".format(api_root, namespace, workspace)
    return http.request(uri, "PUT")


def get_workspace_method_configs(namespace, workspace,api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/methodconfigs".format(api_root, namespace, workspace)
    return http.request(uri)

def create_method_config(namespace, workspace,api_root=PROD_API_ROOT):
    raise NotImplementedError

def update_attributes(namespace, workspace, attr_updates, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/updateAttributes".format(api_root,
                                                           namespace, 
                                                           workspace)
    headers = {"Content-type":  "application/json"}
    json_body = json.dumps(attr_updates)
    return http.request(uri, "PATCH", headers=headers, body=json_body)

def upload_entities(namespace, workspace, entities_tsv, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    with open(entities_tsv, "r") as tsv:
        entity_data = tsv.read()
    request_body = urllib.urlencode({"entities" : entity_data})
    headers = {'Content-type':  "application/x-www-form-urlencoded"}
    uri = "{0}/workspaces/{1}/{2}/importEntities".format(api_root, 
                                                         namespace, 
                                                         workspace)
    return http.request(uri, "POST", headers=headers, body=request_body)

def get_submissions(namespace, workspace, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/submissions".format(api_root, 
                                                      namespace,
                                                      workspace)
    return http.request(uri)

def post_submission(namespace, workspace, api_root=PROD_API_ROOT):
    raise NotImplementedError

def abort_sumbission(namespace, workspace, submission_id, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}".format(api_root, 
                                                        namespace, 
                                                        workspace,
                                                        submission_id)
    return http.request(uri, "DELETE")

def monitor_submission(namespace, workspace, submission_id, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}".format(api_root, 
                                                        namespace, 
                                                        workspace,
                                                        submission_id)
    return http.request(uri)

def get_workflow_outputs(namespace, workspace, submission_id, workflow_id, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/".format(api_root, namespace, workspace)
    uri += "submissions/{0}/workflows/{1}/outputs".format(submission_id, workflow_id)
    return http.request(uri) 

def workspace_entity_types(namespace, workspace, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities".format(api_root, namespace, workspace)
    return http.request(uri)

def get_workspace_entities_with_type(namespace, workspace, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities_with_type".format(api_root, namespace, workspace)
    return http.request(uri)

def get_workspace_entities(namespace, workspace, etype, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}".format(api_root, namespace,
                                                       workspace, etype)
    return http.request(uri)

def delete_entity(namespace, workspace, etype, ename, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/{4}".format(api_root,
                                                         namespace, workspace,
                                                         etype, ename)
    return http.request(uri, "DELETE")

def delete_participant(namespace, workspace, name, api_root=PROD_API_ROOT):
    return delete_entity(namespace, workspace, "participant",
                         name, api_root)

def delete_participant_set(namespace, workspace, name, api_root=PROD_API_ROOT):
    return delete_entity(namespace, workspace, "participant_set", 
                         name, api_root)

def delete_sample(namespace, workspace, name, api_root=PROD_API_ROOT):
    return delete_entity(namespace, workspace, "sample", name, api_root)

def delete_sample_set(namespace, workspace, name, api_root=PROD_API_ROOT):
    return delete_entity(namespace, workspace, "sample_set", name, api_root)

def get_status(api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/status".format(api_root)
    return http.request(uri)

def ping(api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/status/ping".format(api_root)
    return http.request(uri)

def get_repository_methods(api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/methods".format(api_root)
    return http.request(uri)

def get_repository_configurations(api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/configurations".format(api_root)
    return http.request(uri)

def get_config_template(namespace, method, version, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/template".format(api_root)
    headers = {"Content-type":  "application/json"}
    body_dict = {"methodNamespace": namespace, 
                 "methodName": method,
                 "methodVersion": version}
    json_body = json.dumps(body_dict)
    return http.request(uri, "POST", headers=headers, body=json_body)

def get_inputs_outputs(namespace, method, version, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/inputsOutputs".format(api_root)
    headers = {"Content-type":  "application/json"}
    body_dict = {"methodNamespace": namespace, 
                 "methodName": method,
                 "methodVersion": version}
    json_body = json.dumps(body_dict)
    return http.request(uri, "POST", headers=headers, body=json_body)

def get_repository_configuration(namespace, name, snapshot_id, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/configurations/{1}/{2}/{3}".format(api_root,
                                                  namespace,
                                                  name,
                                                  snapshot_id)
    return http.request(uri)

def get_repository_method_acl(namespace, name, snapshot_id, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/methods/{1}/{2}/{3}/permissions".format(api_root,
                                                  namespace,
                                                  name,
                                                  snapshot_id)
    return http.request(uri)

def update_repository_method_acl(namespace, name, snapshot_id, acl_updates, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    json_body = json.dumps(acl_updates)
    uri = "{0}/methods/{1}/{2}/{3}/permissions".format(api_root,
                                                  namespace,
                                                  name,
                                                  snapshot_id)
    return http.request(uri, "POST", headers=headers, body=json_body)

def get_repository_configuration_acl(namespace, name, snapshot_id, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/configurations/{1}/{2}/{3}/permissions".format(api_root,
                                                  namespace,
                                                  name,
                                                  snapshot_id)
    return http.request(uri)

def update_repository_configuration_acl(namespace, name, snapshot_id,
                                        acl_updates, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    headers = {"Content-type":  "application/json"}
    json_body = json.dumps(acl_updates)
    uri = "{0}/configurations/{1}/{2}/{3}/permissions".format(api_root,
                                                  namespace,
                                                  name,
                                                  snapshot_id)
    return http.request(uri, "POST", headers=headers, body=json_body)

def push_workflow(namespace, name, synopsis,
                  wdl, doc=None, api_root=PROD_API_ROOT):
    with open(wdl, 'r') as wf:
        wdl_payload = wf.read()
    if doc is not None:
        with open (doc, 'r') as df:
            doc = df.read()
    else:
        doc = ""

    add_dict = {"namespace": namespace,
                "name": name,
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

def redact_workflow(namespace, name, snapshot_id, api_root=PROD_API_ROOT):
    http = _gcloud_authorized_http()
    uri = "{0}/methods/{1}/{2}/{3}".format(api_root, namespace, 
                                           name, snapshot_id)
    return http.request(uri, "DELETE")
