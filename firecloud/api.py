"""
This module provides python bindings for the Firecloud API.
For more details see https://software.broadinstitute.org/firecloud/

To see how the python bindings map to the RESTful endpoints view
the README at https://pypi.python.org/pypi/firecloud.
"""
import json
import sys
import io
from collections import Iterable

from six import print_
from six.moves.urllib.parse import urlencode
import requests
from oauth2client.client import GoogleCredentials

from firecloud.errors import FireCloudServerError
from firecloud.__about__ import __version__

PROD_API_ROOT = "https://api.firecloud.org/api"
FISS_USER_AGENT = "FISS/" + __version__
__verbosity = 0

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

def __get(uri, headers=None, **kwargs):
    if not headers:
        headers = _fiss_access_headers()
    r = requests.get(uri, headers=headers, **kwargs)
    if __verbosity:
        print_('FISSFC call: %s' % r.url, file=sys.stderr)
    return r

def __post(uri, headers=None, **kwargs):
    if not headers:
        headers = _fiss_access_headers({"Content-type":  "application/json"})
    r = requests.post(uri, headers=headers, **kwargs)
    if __verbosity:
        info = r.url
        json = kwargs.get("json", None)
        if json:
            info += " \n(json=%s) " % json
        print_('FISSFC call: POST %s' % info, file=sys.stderr)
    return r

def __put(uri, headers=None, **kwargs):
    if not headers:
        headers = _fiss_access_headers()
    r = requests.put(uri, headers=headers, **kwargs)
    if __verbosity:
        info = r.url
        json = kwargs.get("json", None)
        if json:
            info += " \n(json=%s) " % json
        print_('FISSFC call: POST %s' % info, file=sys.stderr)
    return r

def __delete(uri, headers=None):
    if not headers:
        headers = _fiss_access_headers()
    r = requests.delete(uri, headers=headers)
    if __verbosity:
        print_('FISSFC call: DELETE %s' % r.url, file=sys.stderr)
    return r

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
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntitiesWithType
    """
    uri = "{0}/workspaces/{1}/{2}/entities_with_type".format(api_root,
                                                    namespace, workspace)
    return __get(uri)

def list_entity_types(namespace, workspace, api_root=PROD_API_ROOT):
    """List the entity types present in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntityTypes
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/workspaces/{1}/{2}/entities".format(
                                api_root, namespace, workspace)
    return __get(uri, headers=headers)

def upload_entities(namespace, workspace,
                    entity_data, api_root=PROD_API_ROOT):
    """Upload entities from tab-delimited string.

    Args:
        namespace (str): project to which workspace belongs
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
    return __post(uri, headers=headers, data=body)

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
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        entities_tsv (file): FireCloud loadfile, see format above
        api_root (str): FireCloud API url, if not production
    """
    if isinstance(entities_tsv, str):
        with open(entities_tsv, "r") as tsv:
            entity_data = tsv.read()
    elif isinstance(entities_tsv, io.StringIO):
        entity_data = entities_tsv.getvalue()
    else:
        raise ValueError('Unsupported input type.')
    return upload_entities(namespace, workspace, entity_data, api_root)

def copy_entities(from_namespace, from_workspace, to_namespace,
                  to_workspace, etype, enames, api_root=PROD_API_ROOT):
    """Copy entities between workspaces

    Args:
        from_namespace (str): project (namespace) to which source workspace belongs
        from_workspace (str): Source workspace name
        to_namespace (str): project (namespace) to which target workspace belongs
        to_workspace (str): Target workspace name
        etype (str): Entity type
        enames (list(str)): List of entity names to copy
        api_root(str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/copyEntities
    """

    uri = "{0}/workspaces/{1}/{2}/entities/copy".format(
                        api_root, to_namespace, to_workspace)
    body = {
        "sourceWorkspace": {
            "namespace": from_namespace,
            "name": from_workspace
        },
        "entityType": etype,
        "entityNames": enames
    }

    return __post(uri, json=body)

def get_entities(namespace, workspace, etype, api_root=PROD_API_ROOT):
    """List entities of given type in a workspace.

    Response content will be in JSON format.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype (str): Entity type
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntities
    """
    uri = "{0}/workspaces/{1}/{2}/entities/{3}".format(
                            api_root, namespace, workspace, etype)
    return __get(uri)

def get_entities_tsv(namespace, workspace, etype, api_root=PROD_API_ROOT):
    """List entities of given type in a workspace as a TSV.

    Identical to get_entities(), but the response is a TSV.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype (str): Entity type
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/browserDownloadEntitiesTSV
    """
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/tsv".format(
                            api_root, namespace, workspace, etype)
    return __get(uri)

def get_entity(namespace, workspace, etype, ename, api_root=PROD_API_ROOT):
    """Request entity information.

    Gets entity metadata and attributes.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype (str): Entity type
        ename (str): The entity's unique id
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntity
    """
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/{4}".format(
                        api_root, namespace, workspace, etype, ename)
    return __get(uri)


def delete_entities(namespace, workspace, json_body, api_root=PROD_API_ROOT):
    """Delete entities in a workspace.

    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        json_body:
        [
          {
            "entityType": "string",
            "entityName": "string"
          }
        ]
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/deleteEntities
    """

    uri = "{0}/workspaces/{1}/{2}/entities/delete".format(
                                api_root, namespace, workspace)
    return __post(uri, json=json_body)

def delete_entity_type(namespace, workspace, etype, ename, api_root=PROD_API_ROOT):
    """Delete entities in a workspace.

    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype (str): Entity type
        ename (str, or iterable of str): unique entity id(s)
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/deleteEntities
    """

    uri = "{0}/workspaces/{1}/{2}/entities/delete".format(
                                        api_root, namespace, workspace)
    if isinstance(ename, str):
        body = [{"entityType":etype, "entityName":ename}]
    elif isinstance(ename, Iterable):
        body = [{"entityType":etype, "entityName":i} for i in ename]
    
    return __post(uri, json=body)

def delete_participant(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete participant in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): participant_id(s)
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity_type(namespace, workspace, "participant", name, api_root)

def delete_participant_set(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete participant set in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): participant_set_id(s)
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity_type(namespace, workspace, "participant_set", name, api_root)

def delete_sample(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete sample in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): sample_id(s)
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity_type(namespace, workspace, "sample", name)

def delete_sample_set(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete sample set in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): sample_set_id(s)
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity_type(namespace, workspace, "sample_set", name, api_root)

def delete_pair(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete pair in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): pair_id(s)
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity_type(namespace, workspace, "pair", name, api_root)

def delete_pair_set(namespace, workspace, name, api_root=PROD_API_ROOT):
    """Delete pair set in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): pair_set_id(s)
        api_root (str): FireCloud API url, if not production
    """
    return delete_entity_type(namespace, workspace, "pair_set", name, api_root)

def get_entities_query(namespace, workspace, etype, page=1,
                       page_size=100, sort_direction="asc",
                       filter_terms=None, api_root=PROD_API_ROOT):
    """Paginated version of get_entities_with_type.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/entityQuery

    """

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
    return __get(uri, params=params)

def update_entity(namespace, workspace, etype, ename,
                  updates, api_root=PROD_API_ROOT):
    """ Update entity attributes in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype     (str): Entity type
        ename     (str): Entity name
        updates   (list(dict)): List of updates to entity from _attr_set, e.g.
        api_root  (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Entities/update_entity
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    uri = "{0}/workspaces/{1}/{2}/entities/{3}/{4}".format(
        api_root, namespace, workspace, etype, ename
    )

    return requests.patch(uri, headers=headers, json=updates)

###############################
### 1.2 Method Configurations
###############################

def list_workspace_configs(namespace, workspace, api_root=PROD_API_ROOT):
    """List method configurations in workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/listWorkspaceMethodConfigs
        DUPLICATE: https://api.firecloud.org/#!/Workspaces/listWorkspaceMethodConfigs
    """
    uri = "{0}/workspaces/{1}/{2}/methodconfigs".format(api_root,
                                                        namespace, workspace)
    return __get(uri)

def create_workspace_config(namespace, workspace, body, api_root=PROD_API_ROOT):
    """Create method configuration in workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        body (json) : a filled-in JSON object for the new method config
                      (e.g. see return value of get_workspace_config)
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/postWorkspaceMethodConfig
        DUPLICATE: https://api.firecloud.org/#!/Workspaces/postWorkspaceMethodConfig
    """

    #json_body = {
    #    "namespace"      : mnamespace,
    #    "name"           : method,
    #    "rootEntityType" : root_etype,
    #    "inputs" : {},
    #    "outputs" : {},
    #    "prerequisites" : {}
    #}
    uri = "{0}/workspaces/{1}/{2}/methodconfigs".format(api_root,
                                                        namespace, workspace)
    return __post(uri, json=body)

def delete_workspace_config(namespace, workspace, cnamespace,
                  config, api_root=PROD_API_ROOT):
    """Delete method configuration in workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        mnamespace (str): Method namespace
        method (str): Method name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/deleteWorkspaceMethodConfig
    """
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}".format(
                    api_root, namespace, workspace, cnamespace, config)
    return __delete(uri)

def get_workspace_config(namespace, workspace, cnamespace,
               config, api_root=PROD_API_ROOT):
    """Get method configuration in workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        cnamespace (str): Config namespace
        config (str): Config name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/getWorkspaceMethodConfig
    """

    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}".format(
                        api_root, namespace, workspace, cnamespace, config)
    return __get(uri)

def update_workspace_config(namespace, workspace, cnamespace,
                            configname, body, api_root=PROD_API_ROOT):
    """Update method configuration in workspace.

    Args:
        namespace  (str): project to which workspace belongs
        workspace  (str): Workspace name
        cnamespace (str): Configuration namespace
        configname (str): Configuration name
        body      (json): new body (definition) of the method config
        api_root   (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/updateWorkspaceMethodConfig
    """
    headers = _fiss_access_headers({"Content-type":  "application/json"})
    body = json.dumps(body)
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}".format(
                        api_root, namespace, workspace, cnamespace, configname)
    return __put(uri, headers=headers, data=body)

def validate_config(namespace, workspace, cnamespace,
                    config, api_root=PROD_API_ROOT):
    """Get syntax validation for a configuration.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        cnamespace (str): Configuration namespace
        config (str): Configuration name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/validate_method_configuration
    """
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}/validate".format(
                        api_root, namespace, workspace, cnamespace, config)
    return __get(uri)

def rename_workspace_config(namespace, workspace, cnamespace,
                  config, new_namespace, new_name, api_root=PROD_API_ROOT):
    """Rename a method configuration in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        mnamespace (str): Config namespace
        config (str): Config name
        new_namespace (str): Updated config namespace
        new_name (str): Updated method name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/renameWorkspaceMethodConfig
    """

    body = {
        "namespace" : new_namespace,
        "name"      : new_name,
        # I have no idea why this is required by FC, but it is...
        "workspaceName" : {
            "namespace" : namespace,
            "name"      : workspace
        }
    }
    uri = "{0}/workspaces/{1}/{2}/method_configs/{3}/{4}/rename".format(
                            api_root, namespace, workspace, cnamespace, config)
    return __post(uri, json=body)

def copy_config_from_repo(namespace, workspace, from_cnamespace,
                          from_config, from_snapshot_id, to_cnamespace,
                          to_config, api_root=PROD_API_ROOT):
    """Copy a method config from the methods repository to a workspace.

    Args:
        namespace (str): project to which workspace belongs
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

    body = {
        "configurationNamespace"  : from_cnamespace,
        "configurationName"       : from_config,
        "configurationSnapshotId" : from_snapshot_id,
        "destinationNamespace"    : to_cnamespace,
        "destinationName"         : to_config
    }
    uri = "{0}/workspaces/{1}/{2}/method_configs/copyFromMethodRepo".format(
                                                api_root, namespace, workspace)
    return __post(uri, json=body)

def copy_config_to_repo(namespace, workspace, from_cnamespace,
                        from_config, to_cnamespace, to_config,
                        api_root=PROD_API_ROOT):
    """Copy a method config from a workspace to the methods repository.

    Args:
        namespace (str): project to which workspace belongs
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

    body = {
        "configurationNamespace" : to_cnamespace,
        "configurationName"      : to_config,
        "sourceNamespace"        : from_cnamespace,
        "sourceName"             : from_config
    }
    uri = "{0}/workspaces/{1}/{2}/method_configs/copyToMethodRepo".format(
                            api_root, namespace, workspace)
    return __post(uri, json=body)

###########################
### 1.3 Method Repository
###########################

def list_repository_methods(api_root=PROD_API_ROOT):
    """List methods in the methods repository.

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/listMethodRepositoryMethods
    """
    uri = "{0}/methods".format(api_root)
    return __get(uri)

def list_repository_configs(api_root=PROD_API_ROOT):
    """List configurations in the methods repository.

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/listMethodRepositoryConfigurations
    """
    uri = "{0}/configurations".format(api_root)
    return __get(uri)

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

    uri = "{0}/template".format(api_root)
    body = {
        "methodNamespace" : namespace,
        "methodName"      : method,
        "methodVersion"   : version
    }
    return __post(uri, json=body)

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

    uri = "{0}/inputsOutputs".format(api_root)
    body = {
        "methodNamespace" : namespace,
        "methodName"      : method,
        "methodVersion"   : snapshot_id
    }
    return __post(uri, json=body)

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
    uri = "{0}/configurations/{1}/{2}/{3}".format(
                    api_root, namespace, config, snapshot_id)
    return __get(uri)

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
    uri = "{0}/methods/{1}/{2}/{3}".format(
                api_root, namespace, method, snapshot_id)
    return __get(uri)

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
        https://api.firecloud.org/#!/Method_Repository/post_api_methods

    """
    with open(wdl, 'r') as wf:
        wdl_payload = wf.read()
    if doc is not None:
        with open (doc, 'r') as df:
            doc = df.read()
    else:
        doc = ""

    body = {
        "namespace": namespace,
        "name": method,
        "entityType": "Workflow",
        "payload": wdl_payload,
        "documentation": doc,
        "synopsis": synopsis
    }

    uri = "{0}/methods".format(api_root)
    return __post(uri, json=body)

def delete_repository_method(namespace, name, snapshot_id,
                             api_root=PROD_API_ROOT):
    """Redacts a method and all of its associated configurations.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/delete_api_methods_namespace_name_snapshotId
    """
    uri = "{0}/methods/{1}/{2}/{3}".format(api_root, namespace, name, snapshot_id)
    return __delete(uri)

def delete_repository_config(namespace, name, snapshot_id,
                             api_root=PROD_API_ROOT):
    """Redacts a configuration and all of its associated configurations.

    The configuration should exist in the methods repository.

    Args:
        namespace (str): configuration namespace
        configuration (str): configuration name
        snapshot_id (int): snapshot_id of the configuration
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/delete_api_configurations_namespace_name_snapshotId
    """
    uri = "{0}/configurations/{1}/{2}/{3}".format(api_root, namespace, name, snapshot_id)
    return __delete(uri)

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
    uri = "{0}/methods/{1}/{2}/{3}/permissions".format(
                        api_root, namespace, method, snapshot_id)
    return __get(uri)

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

    uri = "{0}/methods/{1}/{2}/{3}/permissions".format(
                                api_root, namespace, method, snapshot_id)
    return __post(uri, json=acl_updates)

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
    uri = "{0}/configurations/{1}/{2}/{3}/permissions".format(
                        api_root, namespace, config, snapshot_id)
    return __get(uri)

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

    uri = "{0}/configurations/{1}/{2}/{3}/permissions".format(
                            api_root, namespace, config, snapshot_id)
    return __post(uri, json=acl_updates)

#################
### 1.4 Profile
#################

def list_billing_projects(api_root=PROD_API_ROOT):
    """Get activation information for the logged-in user.

    Swagger:
        https://api.firecloud.org/#!/Profile/billing
    """
    return __get("{0}/profile/billing".format(api_root))

################
### 1.5 Status
################

def get_status(api_root=PROD_API_ROOT):
    """Request the status of FireCloud services.

    Swagger:
        https://api.firecloud.org/#!/Status/status
    """
    return __get("{0}/status".format(api_root))

def ping(api_root=PROD_API_ROOT):
    """Ping FireCloud API.

    Swagger:
        https://api.firecloud.org/#!/Status/ping
    """
    return __get("{0}/status/ping".format(api_root))

######################
### 1.6 Submissions
######################

def list_submissions(namespace, workspace, api_root=PROD_API_ROOT):
    """List submissions in FireCloud workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/listSubmissions
    """
    uri = "{0}/workspaces/{1}/{2}/submissions".format(
                            api_root, namespace, workspace)
    return __get(uri)

def create_submission(wnamespace, workspace, cnamespace, config,
                      entity, etype, expression=None, use_callcache=True, api_root=PROD_API_ROOT):
    """Submit job in FireCloud workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        cnamespace (str): Method configuration namespace
        config (str): Method configuration name
        entity (str): Entity to submit job on. Should be the same type as
            the root entity type of the method config, unless an
            expression is used
        etype (str): Entity type of root_entity
        expression (str): Instead of using entity as the root entity,
            evaluate the root entity from this expression.
        use_callcache (bool): use call cache if applicable (default: true)
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/createSubmission
    """

    uri = "{0}/workspaces/{1}/{2}/submissions".format(
                                api_root, wnamespace, workspace)
    body = {
        "methodConfigurationNamespace" : cnamespace,
        "methodConfigurationName" : config,
         "entityType" : etype,
         "entityName" : entity,
         "useCallCache" : use_callcache
    }

    if expression:
        body['expression'] = expression

    return __post(uri, json=body)

def abort_submission(namespace, workspace,
                     submission_id, api_root=PROD_API_ROOT):
    """Abort running job in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/deleteSubmission
    """
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}".format(
                    api_root, namespace, workspace, submission_id)
    return __delete(uri)

def get_submission(namespace, workspace,
                   submission_id, api_root=PROD_API_ROOT):
    """Request submission information.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/monitorSubmission
    """
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}".format(
                        api_root, namespace, workspace, submission_id)
    return __get(uri)

def get_workflow_metadata(namespace, workspace,
                 submission_id, workflow_id, api_root=PROD_API_ROOT):
    """Request the metadata for a workflow in a submission.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        workflow_id (str): Workflow's unique identifier.
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowMetadata
    """
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}/workflows/{4}".format(
                api_root, namespace, workspace, submission_id, workflow_id)
    return __get(uri)

def get_workflow_outputs(namespace, workspace,
                         submission_id, workflow_id, api_root=PROD_API_ROOT):
    """Request the outputs for a workflow in a submission.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        workflow_id (str): Workflow's unique identifier.
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowOutputsInSubmission
    """
    uri = "{0}/workspaces/{1}/{2}/".format(api_root, namespace, workspace)
    uri += "submissions/{0}/workflows/{1}/outputs".format(
                                    submission_id, workflow_id)
    return __get(uri)

def get_submission_queue(api_root=PROD_API_ROOT):
    """ List workflow counts by queueing state.

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowQueueStatus
    """
    uri = "{0}/submissions/queueStatus".format(api_root)
    return __get(uri)

#####################
### 1.7 Workspaces
#####################

def list_workspaces(api_root=PROD_API_ROOT):
    """Request list of FireCloud workspaces.

    Swagger:
        https://api.firecloud.org/#!/Workspaces/listWorkspaces
    """
    return __get(api_root+ "/workspaces")

def create_workspace(namespace, name, authorizationDomain = "",
                     attributes=None, api_root=PROD_API_ROOT):
    """Create a new FireCloud Workspace.

    Args:
        namespace (str): project to which workspace belongs
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
    if not attributes:
        attributes = dict()
    
    body = {
        "namespace": namespace,
        "name": name,
        "attributes": attributes        
    }
    if authorizationDomain:
        body["authorizationDomain"] = {"membersGroupName": authorizationDomain}

    return __post(uri, json=body)

def delete_workspace(namespace, workspace,api_root=PROD_API_ROOT):
    """Delete FireCloud Workspace.

    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/deleteWorkspace
    """
    uri = "{0}/workspaces/{1}/{2}".format(api_root, namespace, workspace)
    return __delete(uri)

def get_workspace(namespace, workspace,api_root=PROD_API_ROOT):
    """Request FireCloud Workspace information.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/getWorkspace
    """
    uri = "{0}/workspaces/{1}/{2}".format(api_root, namespace, workspace)
    return __get(uri)

def get_workspace_acl(namespace, workspace,api_root=PROD_API_ROOT):
    """Request FireCloud access aontrol list for workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/getWorkspaceAcl
    """
    uri = "{0}/workspaces/{1}/{2}/acl".format(api_root, namespace, workspace)
    return __get(uri)

def update_workspace_acl(namespace, workspace,
                         acl_updates, api_root=PROD_API_ROOT):
    """Update workspace access control list.

    Args:
        namespace (str): project to which workspace belongs
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
        from_namespace (str):  project (namespace) to which source workspace belongs
        from_workspace (str): Source workspace's name
        to_namespace (str):  project to which target workspace belongs
        to_workspace (str): Target workspace's name
        api_root(str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/cloneWorkspace
    """

    body = {
        "namespace": to_namespace,
        "name": to_workspace,
        "attributes": dict()
    }
    uri = "{0}/workspaces/{1}/{2}/clone".format(api_root,
                                               from_namespace,
                                               from_workspace)
    return __post(uri, json=body)

def lock_workspace(namespace, workspace, api_root=PROD_API_ROOT):
    """Lock FireCloud workspace, making it read-only.

    This prevents modifying attributes or submitting workflows
    in the workspace. Can be undone with unlock_workspace()

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/lockWorkspace
    """
    uri = "{0}/workspaces/{1}/{2}/lock".format(api_root, namespace, workspace)
    return __put(uri)

def unlock_workspace(namespace, workspace, api_root=PROD_API_ROOT):
    """Unlock FireCloud workspace.

    Enables modifications to a workspace. See lock_workspace()

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production

    Swagger:
        https://api.firecloud.org/#!/Workspaces/unlockWorkspace
    """
    uri = "{0}/workspaces/{1}/{2}/unlock".format(api_root,
                                                 namespace, workspace)
    return __put(uri)

def update_workspace_attributes(namespace, workspace,
                                attrs, api_root=PROD_API_ROOT):
    """Update or remove workspace attributes.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        api_root (str): FireCloud API url, if not production
        attrs (list(dict)): List of update operations for workspace attributes.
            Use the helper dictionary construction functions to create these:

            _attr_set()      : Set/Update attribute
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

def _attr_set(attr, value):
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

def set_verbosity(verbosity):
    global __verbosity
    previous_value = __verbosity
    try:
        __verbosity = int(verbosity)
    except Exception:
        pass                            # simply keep previous value
    return previous_value

def get_verbosity():
    return __verbosity
