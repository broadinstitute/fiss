"""
This module provides python bindings for the Firecloud API.
For more details see https://software.broadinstitute.org/firecloud/

To see how the python bindings map to the RESTful endpoints view
the README at https://pypi.python.org/pypi/firecloud.
"""
import json
import sys

from six import print_
from six.moves.urllib.parse import urlencode
import requests
from oauth2client.client import GoogleCredentials

from firecloud.errors import FireCloudServerError
from firecloud.__about__ import __version__


PROD_API_ROOT = "https://api.firecloud.org/api"
FISS_USER_AGENT = "FISS/" + __version__
#################################################
# Utilities
#################################################
def _fiss_access_headers(headers=None):
    """ Return request headers for fiss.
        Retrieves an access token with the user's google crededentials, and
        inserts FISS as the User-Agent.

    Args:
        headers (dict): Include additional headers as key-value pairs

    """
    credentials = GoogleCredentials.get_application_default()
    access_token = credentials.get_access_token().access_token
    fiss_headers = {"Authorization" : "bearer " + access_token}
    fiss_headers["User-Agent"] = FISS_USER_AGENT
    if headers:
        fiss_headers.update(headers)
    return fiss_headers

def _check_response_code(response, codes):
    """
    Throws an exception if the http response is not expected. Can check single
    integer or list of valid responses.

    Example usage:
        >>> r = api.get_workspace("broad-firecloud-testing", "Fake-Bucket")
        >>> _check_response_code(r, 200)
         ... FireCloudServerError ...
    """
    if type(codes) == int:
        codes = [codes]
    if response.status_code not in codes:
        raise FireCloudServerError(response.status_code, response.content)

##############################################################
# 1. Orchestration API calls, see https://api.firecloud.org/
##############################################################

##################
### 1.1 Entities
##################
def get_entities_with_type(namespace, workspace,
                           api_root=PROD_API_ROOT):
    """List entities in a workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntitiesWithType
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/entities_with_type".format(
        api_root, namespace, workspace)
    return requests.get(uri, headers=headers)


def list_entity_types(namespace, workspace, api_root=PROD_API_ROOT):
    """List the entity types present in a workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntityTypes
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/workspaces/{1}/{2}/entities".format(
        api_root, namespace, workspace)
    return requests.get(uri, headers=headers)


def upload_entities(namespace, workspace,
                    entity_data, api_root=PROD_API_ROOT):
    """Upload entities from tab-delimited string.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        entity_data (str): TSV string describing entites
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/importEntities
    """
    body = urlencode({"entities" : entity_data})
    headers = _fiss_access_headers({
        'Content-type':  "application/x-www-form-urlencoded"
    })
    uri = "{0}/workspaces/{1}/{2}/importEntities".format(api_root,
                                                         namespace,
                                                         workspace)
    return requests.post(uri, headers=headers, data=body)


def upload_entities_tsv(namespace, workspace,
                        entities_tsv, api_root=PROD_API_ROOT):
    """Upload entities from a tsv loadfile.

    File-based wrapper for api.upload_entities().
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
        entities_tsv (file): FireCloud loadfile, see format above
        api_root (str): FireCloud API url, if not production
    """
    with open(entities_tsv, "r") as tsv:
        entity_data = tsv.read()
        return upload_entities(namespace, workspace, entity_data, api_root)


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

    Swagger:
        https://api.firecloud.org/#!/Entities/copyEntities
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/workspaces/{1}/{2}/entities/copy".format(
        api_root, to_namespace, to_workspace)

    json_body = {
        "sourceWorkspace": {
            "namespace": from_namespace,
            "name": from_workspace
        },
        "entityType": etype,
        "entityNames": enames
    }

    return requests.post(uri, headers=headers, json=json_body)


def get_entities(namespace, workspace, etype, api_root=PROD_API_ROOT):
    """List entities of given type in a workspace.

    Response content will be in JSON format.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        etype (str): Entity type
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntities
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}".format(
        api_root, namespace, workspace, etype)
    return requests.get(uri, headers=headers)


def get_entities_tsv(namespace, workspace, etype, api_root=PROD_API_ROOT):
    """List entities of given type in a workspace as a TSV.

    Identical to get_entities(), but the response is a TSV.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        etype (str): Entity type
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/browserDownloadEntitiesTSV
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/tsv".format(
        api_root, namespace, workspace, etype)
    return requests.get(uri, headers=headers)


def get_entity(namespace, workspace, etype, ename, api_root=PROD_API_ROOT):
    """Request entity information.

    Gets entity metadata and attributes.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        etype (str): Entity type
        ename (str): The entity's unique id
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntity
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/{4}".format(
        api_root, namespace, workspace, etype, ename)
    return requests.get(uri, headers=headers)


## This method is undocumented in the public swagger
def delete_entity(namespace, workspace, etype, ename, api_root=PROD_API_ROOT):
    """Delete entity in a workspace.

    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        etype (str): Entity type
        ename (str): The entity's unique id
        api_root (str): FireCloud API url, if not production

    Swagger:
        UNDOCUMENTED
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/{4}".format(
        api_root, namespace, workspace, etype, ename)
    return requests.delete(uri, headers=headers)


def delete_participant(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete participant in a workspace.

    Convenience wrapper for api.delete_entity().
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

    Convenience wrapper for api.delete_entity().
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

    Convenience wrapper for api.delete_entity().
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

    Convenience wrapper for api.delete_entity().
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

    Convenience wrapper for api.delete_entity().
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

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        name (str): pair_set_id
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity(namespace, workspace, "pair_set", name, api_root)


def get_entities_query(namespace, workspace, etype, page=1,
                       page_size=100, sort_direction="asc",
                       filter_terms=None, api_root=PROD_API_ROOT):
    """Paginated version of get_entities_with_type.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/entityQuery

    """
    headers = _fiss_access_headers()
    # Initial parameters for pagination
    params = {
        "page" : page,
        "pageSize" : page_size,
        "sortDirection" : sort_direction
    }
    if filter_terms:
        params['filterTerms'] = filter_terms

    uri = "{0}/workspaces/{1}/{2}/entityQuery/{3}".format(
        api_root, namespace, workspace, etype)
    return requests.get(uri, headers=headers, params=params)


###############################
### 1.2 Method Configurations
###############################
def list_workspace_configs(namespace, workspace, api_root=PROD_API_ROOT):
    """List method configurations in workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/listWorkspaceMethodConfigs
        DUPLICATE: https://api.firecloud.org/#!/Workspaces/listWorkspaceMethodConfigs
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/methodconfigs".format(api_root,
                                                        namespace, workspace)
    return requests.get(uri, headers=headers)


def create_workspace_config(namespace, workspace, mnamespace, method,
                  root_etype, api_root=PROD_API_ROOT):
    """Create method configuration in workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        mnamespace (str): Method namespace
        method (str): Method name
        root_etype (str): Root entity type
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/postWorkspaceMethodConfig
        DUPLICATE: https://api.firecloud.org/#!/Workspaces/postWorkspaceMethodConfig
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    json_body = {
        "namespace"      : mnamespace,
        "name"           : method,
        "rootEntityType" : root_etype
    }
    uri = "{0}/workspaces/{1}/{2}/methodconfigs".format(api_root,
                                                        namespace, workspace)
    return requests.post(uri, headers=headers, json=json_body)


def delete_workspace_config(namespace, workspace, cnamespace,
                  config, api_root=PROD_API_ROOT):
    """Delete method configuration in workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        mnamespace (str): Method namespace
        method (str): Method name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/deleteWorkspaceMethodConfig
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}".format(
        api_root, namespace, workspace, cnamespace, config)
    return requests.delete(uri, headers=headers)


def get_workspace_config(namespace, workspace, cnamespace,
               config, api_root=PROD_API_ROOT):
    """Get method configuration in workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        cnamespace (str): Config namespace
        config (str): Config name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/getWorkspaceMethodConfig

    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}".format(
        api_root, namespace, workspace, cnamespace, config)
    return requests.get(uri, headers=headers)


def update_workspace_config(namespace, workspace, cnamespace,
                            configname, config_updates,
                            api_root=PROD_API_ROOT):
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

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/updateWorkspaceMethodConfig
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    body = json.dumps(config_updates)
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}".format(
        api_root, namespace, workspace, cnamespace, configname)
    return requests.put(uri, headers=headers, data=body)


def validate_config(namespace, workspace, cnamespace,
                    config, api_root=PROD_API_ROOT):
    """Get syntax validation for a configuration.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        cnamespace (str): Configuration namespace
        config (str): Configuration name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/validate_method_configuration
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}/validate".format(
        api_root, namespace, workspace, cnamespace, config)
    return requests.get(uri, headers=headers)


def rename_workspace_config(namespace, workspace, cnamespace,
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

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/renameWorkspaceMethodConfig
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    json_body = {
        "namespace" : new_name,
        "name"      : new_namespace
    }
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}/rename".format(
        api_root, namespace, workspace, cnamespace, config)
    return requests.post(uri, headers=headers, json=json_body)


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

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/copyFromMethodRepo
        DUPLICATE: https://api.firecloud.org/#!/Method_Repository/copyFromMethodRepo
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    json_body = {
        "configurationNamespace"  : from_cnamespace,
        "configurationName"       : from_config,
        "configurationSnapshotId" : from_snapshot_id,
        "destinationNamespace"    : to_cnamespace,
        "destinationName"         : to_config
    }
    uri = "{0}/workspaces/{1}/{2}/method_configs/copyFromMethodRepo".format(
        api_root, namespace, workspace)
    return requests.post(uri, headers=headers, json=json_body)


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

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/copyToMethodRepo
        DUPLICATE: https://api.firecloud.org/#!/Method_Repository/copyToMethodRepo
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    json_body = {
        "configurationNamespace" : to_cnamespace,
        "configurationName"      : to_config,
        "sourceNamespace"        : from_cnamespace,
        "sourceName"             : from_config
    }
    uri = "{0}/workspaces/{1}/{2}/method_configs/copyToMethodRepo".format(
        api_root, namespace, workspace, cnamespace, config)
    return requests.post(uri, headers=headers, json=json_body)


###########################
### 1.3 Method Repository
###########################
def list_repository_methods(api_root=PROD_API_ROOT):
    """List methods in the methods repository.

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/listMethodRepositoryMethods
    """
    headers = _fiss_access_headers()
    uri = "{0}/methods".format(api_root)
    return requests.get(uri, headers=headers)


def list_repository_configs(api_root=PROD_API_ROOT):
    """List configurations in the methods repository.

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/listMethodRepositoryConfigurations
    """
    headers = _fiss_access_headers()
    uri = "{0}/configurations".format(api_root)
    return requests.get(uri, headers=headers)


def get_config_template(namespace, method, version, api_root=PROD_API_ROOT):
    """Get the configuration template for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        version (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/createMethodTemplate
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/template".format(api_root)
    json_body = {
        "methodNamespace" : namespace,
        "methodName"      : method,
        "methodVersion"   : version
    }
    return requests.post(uri, headers=headers, json=json_body)


def get_inputs_outputs(namespace, method, snapshot_id, api_root=PROD_API_ROOT):
    """Get a description of the inputs and outputs for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/getMethodIO
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/inputsOutputs".format(api_root)
    json_body = {
        "methodNamespace" : namespace,
        "methodName"      : method,
        "methodVersion"   : snapshot_id
    }
    return requests.post(uri, headers=headers, json=json_body)


def get_repository_config(namespace, config,
                          snapshot_id, api_root=PROD_API_ROOT):
    """Get a method configuration from the methods repository.

    Args:
        namespace (str): Methods namespace
        config (str): config name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/getMethodRepositoryConfiguration
    """
    headers = _fiss_access_headers()
    uri = "{0}/configurations/{1}/{2}/{3}".format(
        api_root, namespace, config, snapshot_id)
    return requests.get(uri, headers=headers)


def get_repository_method(namespace, method, snapshot_id,
                          api_root=PROD_API_ROOT):
    """Get a method definition from the method repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        version (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production

    Swagger:
        UNDOCUMENTED
    """
    headers = _fiss_access_headers()
    uri = "{0}/methods/{1}/{2}/{3}".format(
        api_root, namespace, method, snapshot_id)
    return requests.get(uri, headers=headers)


def update_repository_method(namespace, method, synopsis,
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

    Swagger:
        UNDOCUMENTED

    """
    with open(wdl, 'r') as wf:
        wdl_payload = wf.read()
    if doc is not None:
        with open (doc, 'r') as df:
            doc = df.read()
    else:
        doc = ""

    json_body = {
        "namespace": namespace,
        "name": method,
        "entityType": "Workflow",
        "payload": wdl_payload,
        "documentation": doc,
        "synopsis": synopsis
    }
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/methods".format(api_root)
    return requests.post(uri, headers=headers, json=json_body)


def delete_repository_method(namespace, name, snapshot_id,
                             api_root=PROD_API_ROOT):
    """Redact a version of a workflow.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production

    Swagger:
        UNDOCUMENTED
    """
    headers = _fiss_access_headers()
    uri = "{0}/methods/{1}/{2}/{3}".format(api_root, namespace,
                                           name, snapshot_id)
    return requests.delete(uri, headers=headers)


def get_repository_method_acl(namespace, method,
                              snapshot_id, api_root=PROD_API_ROOT):
    """Get permissions for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        version (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/getMethodACL
    """
    headers = _fiss_access_headers()
    uri = "{0}/methods/{1}/{2}/{3}/permissions".format(
        api_root, namespace, method, snapshot_id)
    return requests.get(uri, headers=headers)


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

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/setMethodACL
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/methods/{1}/{2}/{3}/permissions".format(
        api_root, namespace, method, snapshot_id)
    return requests.post(uri, headers=headers, json=acl_updates)


def get_repository_config_acl(namespace, config,
                              snapshot_id, api_root=PROD_API_ROOT):
    """Get configuration permissions.

    The configuration should exist in the methods repository.

    Args:
        namespace (str): Configuration namespace
        config (str): Configuration name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/getConfigACL
    """
    headers = _fiss_access_headers()
    uri = "{0}/configurations/{1}/{2}/{3}/permissions".format(
        api_root, namespace, config, snapshot_id)
    return requests.get(uri, headers=headers)


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

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/setConfigACL
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/configurations/{1}/{2}/{3}/permissions".format(
        api_root, namespace, config, snapshot_id)
    return requests.post(uri, headers=headers, json=acl_updates)


#################
### 1.4 Profile
#################
def list_billing_projects(api_root=PROD_API_ROOT):
    """Get activation information for the logged-in user.

    Swagger:
        https://api.firecloud.org/#!/Profile/billing
    """
    headers = _fiss_access_headers()
    uri = "{0}/profile/billing".format(api_root)
    return requests.get(uri, headers=headers)


################
### 1.5 Status
################
def get_status(api_root=PROD_API_ROOT):
    """Request the status of FireCloud services.

    Swagger:
        https://api.firecloud.org/#!/Status/status
    """
    headers = _fiss_access_headers()
    uri = "{0}/status".format(api_root)
    return requests.get(uri, headers=headers)


def ping(api_root=PROD_API_ROOT):
    """Ping API.

    Swagger:
        https://api.firecloud.org/#!/Status/ping
    """
    headers = _fiss_access_headers()
    uri = "{0}/status/ping".format(api_root)
    return requests.get(uri, headers=headers)


######################
### 1.6 Submissions
######################
def list_submissions(namespace, workspace, api_root=PROD_API_ROOT):
    """List submissions in FireCloud workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/listSubmissions
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/submissions".format(
        api_root, namespace, workspace)
    return requests.get(uri, headers=headers)


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
        etype (str): Entity type of root_entity
        expression (str): Instead of using entity as the root entity,
            evaluate the root entity from this expression.
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/createSubmission
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/workspaces/{1}/{2}/submissions".format(
        api_root, wnamespace, workspace)
    json_body = {
        "methodConfigurationNamespace" : cnamespace,
        "methodConfigurationName" : config,
         "entityType" : etype,
         "entityName" : entity,
         "expression" : expression
    }

    return http.post(uri, headers=headers, json=json_body)


def abort_sumbission(namespace, workspace,
                     submission_id, api_root=PROD_API_ROOT):
    """Abort running job in a workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/deleteSubmission
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}".format(
        api_root, namespace, workspace, submission_id)
    return http.delete(uri, headers=headers)


def get_submission(namespace, workspace,
                   submission_id, api_root=PROD_API_ROOT):
    """Request submission information.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/monitorSubmission
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}".format(
        api_root, namespace, workspace, submission_id)
    return requests.get(uri, headers=headers)


def get_workflow_outputs(namespace, workspace,
                         submission_id, workflow_id, api_root=PROD_API_ROOT):
    """Request the outputs for a workflow in a submission.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        workflow_id (str): Workflow's unique identifier.
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowOutputsInSubmission
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/".format(api_root, namespace, workspace)
    uri += "submissions/{0}/workflows/{1}/outputs".format(
        submission_id, workflow_id)
    return requests.get(uri, headers=headers)


def get_submission_queue(namespace, workspace, api_root=PROD_API_ROOT):
    """ List workflow counts by queueing state.

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowQueueStatus
    """
    headers = _fiss_access_headers()
    uri = "{0}/submissions/queueStatus/".format(api_root)
    return requests.get(uri, headers=headers)


#####################
### 1.7 Workspaces
#####################
def list_workspaces(api_root=PROD_API_ROOT):
    """Request list of FireCloud workspaces.

    Swagger:
        https://api.firecloud.org/#!/Workspaces/listWorkspaces
    """
    uri = "{0}/workspaces".format(api_root)
    headers = _fiss_access_headers()
    return requests.get(uri, headers=headers)


def create_workspace(namespace, name, protected=False,
                     attributes=None, api_root=PROD_API_ROOT):
    """Create a new FireCloud Workspace.

    Args:
        namespace (str): Google project for the workspace
        name (str): Workspace name
        protected (bool): If True, this workspace is protected by dbGaP
            credentials. This option is only available if your FireCloud
            account is linked to your NIH account.
        attributes (dict): Workspace attributes as key value pairs
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/createWorkspace
    """
    uri = "{0}/workspaces".format(api_root)
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    if not attributes:
        attributes = dict()
    json_body = {
        "namespace": namespace,
        "name": name,
        "attributes": attributes,
        "isProtected": protected
    }
    return requests.post(uri, headers=headers, json=json_body)


def delete_workspace(namespace, workspace,api_root=PROD_API_ROOT):
    """Delete FireCloud Workspace.

    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/deleteWorkspace
    """
    uri = "{0}/workspaces/{1}/{2}".format(api_root, namespace, workspace)
    headers = _fiss_access_headers()
    return requests.delete(uri, headers=headers)


def get_workspace(namespace, workspace,api_root=PROD_API_ROOT):
    """Request FireCloud Workspace information.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/getWorkspace
    """
    uri = "{0}/workspaces/{1}/{2}".format(api_root, namespace, workspace)
    headers = _fiss_access_headers()
    return requests.get(uri, headers=headers)


def get_workspace_acl(namespace, workspace,api_root=PROD_API_ROOT):
    """Request FireCloud access aontrol list for workspace.

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/getWorkspaceAcl
    """
    uri = "{0}/workspaces/{1}/{2}/acl".format(api_root, namespace, workspace)
    headers = _fiss_access_headers()
    return requests.get(uri, headers=headers)


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

    Swagger:
        https://api.firecloud.org/#!/Workspaces/updateWorkspaceACL
    """
    uri = "{0}/workspaces/{1}/{2}/acl".format(api_root, namespace, workspace)
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    return requests.patch(uri, headers=headers, data=json.dumps(acl_updates))


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

    Swagger:
        https://api.firecloud.org/#!/Workspaces/cloneWorkspace
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    json_body = {"namespace": to_namespace,
                 "name": to_workspace,
                 "attributes": dict()}

    uri = "{0}/workspaces/{1}/{2}/clone".format(api_root,
                                               from_namespace,
                                               from_workspace)
    return requests.post(uri, headers=headers, json=json_body)


def lock_workspace(namespace, workspace, api_root=PROD_API_ROOT):
    """Lock FireCloud workspace, making it read-only.

    This prevents modifying attributes or submitting workflows
    in the workspace. Can be undone with unlock_workspace()

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/lockWorkspace
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/lock".format(api_root, namespace, workspace)
    return requests.put(uri, headers=headers)


def unlock_workspace(namespace, workspace, api_root=PROD_API_ROOT):
    """Unlock FireCloud workspace.

    Enables modifications to a workspace. See lock_workspace()

    Args:
        namespace (str): Google project for the workspace
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/unlockWorkspace
    """
    headers = _fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/unlock".format(api_root,
                                                 namespace, workspace)
    return requests.put(uri, headers=headers)


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

    Swagger:
        https://api.firecloud.org/#!/Workspaces/updateAttributes
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/workspaces/{1}/{2}/updateAttributes".format(
        api_root, namespace, workspace)

    body = json.dumps(attrs)

    return requests.patch(uri, headers=headers, data=body)


# Helper functions to create attribute update dictionaries
def _attr_up(attr, value):
    """Create an 'update 'dictionary for update_workspace_attributes()"""
    return {
        "op"                 : "AddUpdateAttribute",
        "attributeName"      : attr,
        "addUpdateAttribute" : value
   }

def _attr_rem(attr):
    """Create a 'remove' dictionary for update_workspace_attributes()"""
    return {
        "op"             : "RemoveAttribute",
        "attributeName"  : attr
    }

def _attr_ladd(attr, value):
    """Create a 'list add' dictionary for update_workspace_attributes()"""
    return {
        "op"                 : "AddListMember",
        "attributeName"      : attr,
        "addUpdateAttribute" : value
    }

def _attr_lrem(attr, value):
    """Create a 'list remove' dictionary for update_workspace_attributes()"""
    return {
        "op"                 : "RemoveListMember",
        "attributeName"      : attr,
        "addUpdateAttribute" : value
    }
