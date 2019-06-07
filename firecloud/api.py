"""
This module provides python bindings for the Firecloud API.
For more details see https://software.broadinstitute.org/firecloud/

To see how the python bindings map to the RESTful endpoints view
the README at https://pypi.python.org/pypi/firecloud.
"""

from __future__ import print_function
import json
import sys
import os
import io
import logging
import subprocess
from collections import Iterable

from six.moves.urllib.parse import urlencode, urljoin
from six import string_types

import google.auth
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2 import id_token

from firecloud.errors import FireCloudServerError
from firecloud.fccore import __fcconfig as fcconfig
from firecloud.__about__ import __version__


FISS_USER_AGENT = "FISS/" + __version__

# Set Global Authorized Session
__SESSION = None
__USER_ID = None

# Suppress warnings about project ID
logging.getLogger('google.auth').setLevel(logging.ERROR)

#################################################
# Utilities
#################################################
def _set_session():
    """ Sets global __SESSION and __USER_ID if they haven't been set """
    global __SESSION
    global __USER_ID
    
    if __SESSION is None:
        try:
            __SESSION = AuthorizedSession(google.auth.default(['https://www.googleapis.com/auth/userinfo.profile',
                                                               'https://www.googleapis.com/auth/userinfo.email'])[0])
            health()
            __USER_ID = id_token.verify_oauth2_token(__SESSION.credentials.id_token,
                                                     Request(session=__SESSION))['email']
        except (DefaultCredentialsError, RefreshError) as gae:
            if os.getenv('SERVER_SOFTWARE', '').startswith('Google App Engine/'):
                raise
            logging.warning("Unable to determine/refresh application credentials")
            try:
                subprocess.check_call(['gcloud', 'auth', 'application-default',
                                       'login', '--no-launch-browser'])
                __SESSION = AuthorizedSession(google.auth.default(['https://www.googleapis.com/auth/userinfo.profile',
                                                                   'https://www.googleapis.com/auth/userinfo.email'])[0])
            except subprocess.CalledProcessError as cpe:
                if cpe.returncode < 0:
                    logging.exception("%s was terminated by signal %d",
                                      cpe.cmd, -cpe.returncode)
                else:
                    logging.exception("%s returned %d", cpe.cmd,
                                      cpe.returncode)
                raise gae

def _fiss_agent_header(headers=None):
    """ Return request headers for fiss.
        Inserts FISS as the User-Agent.
        Initializes __SESSION if it hasn't been set.

    Args:
        headers (dict): Include additional headers as key-value pairs

    """
    _set_session()

    fiss_headers = {"User-Agent" : FISS_USER_AGENT}
    if headers is not None:
        fiss_headers.update(headers)
    return fiss_headers

def __get(methcall, headers=None, root_url=fcconfig.root_url, **kwargs):
    if not headers:
        headers = _fiss_agent_header()
    r = __SESSION.get(urljoin(root_url, methcall), headers=headers, **kwargs)
    if fcconfig.verbosity > 1:
        print('FISSFC call: %s' % r.url, file=sys.stderr)
    return r

def __post(methcall, headers=None, root_url=fcconfig.root_url, **kwargs):
    if not headers:
        headers = _fiss_agent_header({"Content-type":  "application/json"})
    r = __SESSION.post(urljoin(root_url, methcall), headers=headers, **kwargs)
    if fcconfig.verbosity > 1:
        info = r.url
        json = kwargs.get("json", None)
        if json:
            info += " \n(json=%s) " % json
        print('FISSFC call: POST %s' % info, file=sys.stderr)
    return r

def __put(methcall, headers=None, root_url=fcconfig.root_url, **kwargs):
    if not headers:
        headers = _fiss_agent_header()
    r = __SESSION.put(urljoin(root_url, methcall), headers=headers, **kwargs)
    if fcconfig.verbosity > 1:
        info = r.url
        json = kwargs.get("json", None)
        if json:
            info += " \n(json=%s) " % json
        print('FISSFC call: PUT %s' % info, file=sys.stderr)
    return r

def __delete(methcall, headers=None, root_url=fcconfig.root_url):
    if not headers:
        headers = _fiss_agent_header()
    r = __SESSION.delete(urljoin(root_url, methcall), headers=headers)
    if fcconfig.verbosity > 1:
        print('FISSFC call: DELETE %s' % r.url, file=sys.stderr)
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

def whoami():
    """ Return __USER_ID """
    _set_session()
    return __USER_ID

##############################################################
# 1. Orchestration API calls, see https://api.firecloud.org/
##############################################################

##################
### 1.1 Entities
##################

def get_entities_with_type(namespace, workspace):
    """List entities in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntitiesWithType
    """
    uri = "workspaces/{0}/{1}/entities_with_type".format(namespace, workspace)
    return __get(uri)

def list_entity_types(namespace, workspace):
    """List the entity types present in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntityTypes
    """
    headers = _fiss_agent_header({"Content-type":  "application/json"})
    uri = "workspaces/{0}/{1}/entities".format(namespace, workspace)
    return __get(uri, headers=headers)

def upload_entities(namespace, workspace, entity_data, model='firecloud'):
    """Upload entities from tab-delimited string.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        entity_data (str): TSV string describing entites
        model (str): Data Model type. "firecloud" (default) or "flexible"

    Swagger:
        https://api.firecloud.org/#!/Entities/importEntities
        https://api.firecloud.org/#!/Entities/flexibleImportEntities
    """
    body = urlencode({"entities" : entity_data})
    headers = _fiss_agent_header({
        'Content-type':  "application/x-www-form-urlencoded"
    })
    endpoint = 'importEntities'
    if model == 'flexible':
        endpoint = 'flexibleImportEntities'
    uri = "workspaces/{0}/{1}/{2}".format(namespace, workspace, endpoint)
    return __post(uri, headers=headers, data=body)

def upload_entities_tsv(namespace, workspace, entities_tsv, model='firecloud'):
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
        model (str): Data Model type. "firecloud" (default) or "flexible"
    """
    if isinstance(entities_tsv, string_types):
        with open(entities_tsv, "r") as tsv:
            entity_data = tsv.read()
    elif isinstance(entities_tsv, io.StringIO):
        entity_data = entities_tsv.getvalue()
    else:
        raise ValueError('Unsupported input type.')
    return upload_entities(namespace, workspace, entity_data, model)

def copy_entities(from_namespace, from_workspace, to_namespace,
                  to_workspace, etype, enames, link_existing_entities=False):
    """Copy entities between workspaces

    Args:
        from_namespace (str): project (namespace) to which source workspace belongs
        from_workspace (str): Source workspace name
        to_namespace (str): project (namespace) to which target workspace belongs
        to_workspace (str): Target workspace name
        etype (str): Entity type
        enames (list(str)): List of entity names to copy
        link_existing_entities (boolean): Link all soft conflicts to the entities that already exist.

    Swagger:
        https://api.firecloud.org/#!/Entities/copyEntities
    """

    uri = "workspaces/{0}/{1}/entities/copy".format(to_namespace, to_workspace)
    body = {
        "sourceWorkspace": {
            "namespace": from_namespace,
            "name": from_workspace
        },
        "entityType": etype,
        "entityNames": enames
    }
    
    return __post(uri, json=body, params={'linkExistingEntities':
                                          str(link_existing_entities).lower()})

def get_entities(namespace, workspace, etype):
    """List entities of given type in a workspace.

    Response content will be in JSON format.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype (str): Entity type

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntities
    """
    uri = "workspaces/{0}/{1}/entities/{2}".format(namespace, workspace, etype)
    return __get(uri)

def get_entities_tsv(namespace, workspace, etype, attrs=None, model="firecloud"):
    """List entities of given type in a workspace as a TSV.

    Similar to get_entities(), but the response is a TSV.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype (str): Entity type
        attrs (list): list of ordered attribute names to be in downloaded tsv
        model (str): Data Model type. "firecloud" (default) or "flexible"

    Swagger:
        https://api.firecloud.org/#!/Entities/downloadEntitiesTSV
    """
    params = {'model': model}
    if attrs is not None:
        params['attributeNames'] = ','.join(attrs)
    uri = "workspaces/{0}/{1}/entities/{2}/tsv".format(namespace,
                                                workspace, etype)
    return __get(uri, params=params)

def get_entity(namespace, workspace, etype, ename):
    """Request entity information.

    Gets entity metadata and attributes.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype (str): Entity type
        ename (str): The entity's unique id

    Swagger:
        https://api.firecloud.org/#!/Entities/getEntity
    """
    uri = "workspaces/{0}/{1}/entities/{2}/{3}".format(namespace,
                                            workspace, etype, ename)
    return __get(uri)

def delete_entities(namespace, workspace, json_body):
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

    Swagger:
        https://api.firecloud.org/#!/Entities/deleteEntities
    """

    uri = "workspaces/{0}/{1}/entities/delete".format(namespace, workspace)
    return __post(uri, json=json_body)

def delete_entity_type(namespace, workspace, etype, ename):
    """Delete entities in a workspace.

    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype (str): Entity type
        ename (str, or iterable of str): unique entity id(s)

    Swagger:
        https://api.firecloud.org/#!/Entities/deleteEntities
    """

    uri = "workspaces/{0}/{1}/entities/delete".format(namespace, workspace)
    if isinstance(ename, string_types):
        body = [{"entityType":etype, "entityName":ename}]
    elif isinstance(ename, Iterable):
        body = [{"entityType":etype, "entityName":i} for i in ename]

    return __post(uri, json=body)

def delete_participant(namespace, workspace, name):
    """Delete participant in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): participant_id(s)
    """
    return delete_entity_type(namespace, workspace, "participant", name)

def delete_participant_set(namespace, workspace, name):
    """Delete participant set in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): participant_set_id(s)
    """
    return delete_entity_type(namespace, workspace, "participant_set", name)

def delete_sample(namespace, workspace, name):
    """Delete sample in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): sample_id(s)
    """
    return delete_entity_type(namespace, workspace, "sample", name)

def delete_sample_set(namespace, workspace, name):
    """Delete sample set in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): sample_set_id(s)
    """
    return delete_entity_type(namespace, workspace, "sample_set", name)

def delete_pair(namespace, workspace, name):
    """Delete pair in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): pair_id(s)
    """
    return delete_entity_type(namespace, workspace, "pair", name)

def delete_pair_set(namespace, workspace, name):
    """Delete pair set in a workspace.

    Convenience wrapper for api.delete_entity().
    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        name (str, or iterable of str): pair_set_id(s)
    """
    return delete_entity_type(namespace, workspace, "pair_set", name)

def get_entities_query(namespace, workspace, etype, page=1,
                       page_size=100, sort_direction="asc",
                       filter_terms=None):
    """Paginated version of get_entities_with_type.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

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

    uri = "workspaces/{0}/{1}/entityQuery/{2}".format(namespace,workspace,etype)
    return __get(uri, params=params)

def update_entity(namespace, workspace, etype, ename, updates):
    """ Update entity attributes in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        etype     (str): Entity type
        ename     (str): Entity name
        updates   (list(dict)): List of updates to entity from _attr_set, e.g.

    Swagger:
        https://api.firecloud.org/#!/Entities/update_entity
    """
    headers = _fiss_agent_header({"Content-type":  "application/json"})
    uri = "{0}workspaces/{1}/{2}/entities/{3}/{4}".format(fcconfig.root_url,
                                            namespace, workspace, etype, ename)

    # FIXME: create __patch method, akin to __get, __delete etc
    return __SESSION.patch(uri, headers=headers, json=updates)

###############################
### 1.2 Method Configurations
###############################

def list_workspace_configs(namespace, workspace, allRepos=False):
    """List method configurations in workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/listWorkspaceMethodConfigs
        DUPLICATE: https://api.firecloud.org/#!/Workspaces/listWorkspaceMethodConfigs
    """
    uri = "workspaces/{0}/{1}/methodconfigs".format(namespace, workspace)
    return __get(uri, params={'allRepos': allRepos})

def create_workspace_config(namespace, workspace, body):
    """Create method configuration in workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        body (json) : a filled-in JSON object for the new method config
                      (e.g. see return value of get_workspace_config)

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
    uri = "workspaces/{0}/{1}/methodconfigs".format(namespace, workspace)
    return __post(uri, json=body)

def delete_workspace_config(namespace, workspace, cnamespace, config):
    """Delete method configuration in workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        mnamespace (str): Method namespace
        method (str): Method name

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/deleteWorkspaceMethodConfig
    """
    uri = "workspaces/{0}/{1}/method_configs/{2}/{3}".format(namespace,
                                            workspace, cnamespace, config)
    return __delete(uri)

def get_workspace_config(namespace, workspace, cnamespace, config):
    """Get method configuration in workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        cnamespace (str): Config namespace
        config (str): Config name

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/getWorkspaceMethodConfig
    """

    uri = "workspaces/{0}/{1}/method_configs/{2}/{3}".format(namespace,
                                            workspace, cnamespace, config)
    return __get(uri)

def overwrite_workspace_config(namespace, workspace, cnamespace, configname, body):
    """Add or overwrite method configuration in workspace.

    Args:
        namespace  (str): project to which workspace belongs
        workspace  (str): Workspace name
        cnamespace (str): Configuration namespace
        configname (str): Configuration name
        body      (json): new body (definition) of the method config

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/overwriteWorkspaceMethodConfig
    """
    headers = _fiss_agent_header({"Content-type": "application/json"})
    uri = "workspaces/{0}/{1}/method_configs/{2}/{3}".format(namespace,
                                        workspace, cnamespace, configname)
    return __put(uri, headers=headers, json=body)

def update_workspace_config(namespace, workspace, cnamespace, configname, body):
    """Update method configuration in workspace.

    Args:
        namespace  (str): project to which workspace belongs
        workspace  (str): Workspace name
        cnamespace (str): Configuration namespace
        configname (str): Configuration name
        body      (json): new body (definition) of the method config

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/updateWorkspaceMethodConfig
    """
    uri = "workspaces/{0}/{1}/method_configs/{2}/{3}".format(namespace,
                                        workspace, cnamespace, configname)
    return __post(uri, json=body)

def validate_config(namespace, workspace, cnamespace, config):
    """Get syntax validation for a configuration.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        cnamespace (str): Configuration namespace
        config (str): Configuration name

    Swagger:
        https://api.firecloud.org/#!/Method_Configurations/validate_method_configuration
    """
    uri = "workspaces/{0}/{1}/method_configs/{2}/{3}/validate".format(namespace,
                                                    workspace, cnamespace, config)
    return __get(uri)

def rename_workspace_config(namespace, workspace, cnamespace, config,
                                            new_namespace, new_name):
    """Rename a method configuration in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        mnamespace (str): Config namespace
        config (str): Config name
        new_namespace (str): Updated config namespace
        new_name (str): Updated method name

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
    uri = "workspaces/{0}/{1}/method_configs/{2}/{3}/rename".format(namespace,
                                                workspace, cnamespace, config)
    return __post(uri, json=body)

def copy_config_from_repo(namespace, workspace, from_cnamespace,
                          from_config, from_snapshot_id, to_cnamespace,
                          to_config):
    """Copy a method config from the methods repository to a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        from_cnamespace (str): Source configuration namespace
        from_config (str): Source configuration name
        from_snapshot_id (int): Source configuration snapshot_id
        to_cnamespace (str): Target configuration namespace
        to_config (str): Target configuration name

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
    uri = "workspaces/{0}/{1}/method_configs/copyFromMethodRepo".format(
                                                        namespace, workspace)
    return __post(uri, json=body)

def copy_config_to_repo(namespace, workspace, from_cnamespace,
                        from_config, to_cnamespace, to_config):
    """Copy a method config from a workspace to the methods repository.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        from_cnamespace (str): Source configuration namespace
        from_config (str): Source configuration name
        to_cnamespace (str): Target configuration namespace
        to_config (str): Target configuration name

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
    uri = "workspaces/{0}/{1}/method_configs/copyToMethodRepo".format(
                                                    namespace, workspace)
    return __post(uri, json=body)

###########################
### 1.3 Method Repository
###########################

def list_repository_methods(namespace=None, name=None, snapshotId=None):
    """List method(s) in the methods repository.

    Args:
        namespace (str): Method Repository namespace
        name (str): method name
        snapshotId (int): method snapshot ID

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/listMethodRepositoryMethods
    """
    params = {k:v for (k,v) in locals().items() if v is not None}
    return __get("methods", params=params)

def list_repository_configs(namespace=None, name=None, snapshotId=None):
    """List configurations in the methods repository.

    Args:
        namespace (str): Method Repository namespace
        name (str): config name
        snapshotId (int): config snapshot ID

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/listMethodRepositoryConfigurations
    """
    params = {k:v for (k,v) in locals().items() if v is not None}
    return __get("configurations", params=params)

def get_config_template(namespace, method, version):
    """Get the configuration template for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Method's namespace
        method (str): method name
        version (int): snapshot_id of the method

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/createMethodTemplate
    """

    body = {
        "methodNamespace" : namespace,
        "methodName"      : method,
        "methodVersion"   : int(version)
    }
    return __post("template", json=body)

def get_inputs_outputs(namespace, method, snapshot_id):
    """Get a description of the inputs and outputs for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/getMethodIO
    """

    body = {
        "methodNamespace" : namespace,
        "methodName"      : method,
        "methodVersion"   : snapshot_id
    }
    return __post("inputsOutputs", json=body)

def get_repository_config(namespace, config, snapshot_id):
    """Get a method configuration from the methods repository.

    Args:
        namespace (str): Methods namespace
        config (str): config name
        snapshot_id (int): snapshot_id of the method

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/getMethodRepositoryConfiguration
    """
    uri = "configurations/{0}/{1}/{2}".format(namespace, config, snapshot_id)
    return __get(uri)

def get_repository_method(namespace, method, snapshot_id, wdl_only=False):
    """Get a method definition from the method repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        version (int): snapshot_id of the method
        wdl_only (bool): Exclude metadata

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/get_api_methods_namespace_name_snapshotId
    """
    uri = "methods/{0}/{1}/{2}?onlyPayload={3}".format(namespace, method,
                                                       snapshot_id,
                                                       str(wdl_only).lower())
    return __get(uri)

def update_repository_method(namespace, method, synopsis, wdl, doc=None,
                             comment=""):
    """Create/Update workflow definition.

    FireCloud will create a new snapshot_id for the given workflow.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        synopsis (str): short (<80 char) description of method
        wdl (file): Workflow Description Language file
        doc (file): (Optional) Additional documentation
        comment (str): (Optional) Comment specific to this snapshot

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/post_api_methods

    """
    with open(wdl, 'r') as wf:
        wdl_payload = wf.read()
    if doc is not None:
        with open (doc, 'r') as df:
            doc = df.read()

    body = {
        "namespace": namespace,
        "name": method,
        "entityType": "Workflow",
        "payload": wdl_payload,
        "documentation": doc,
        "synopsis": synopsis,
        "snapshotComment": comment
    }

    return __post("methods",
                  json={key: value for key, value in body.items() if value})

def delete_repository_method(namespace, name, snapshot_id):
    """Redacts a method and all of its associated configurations.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/delete_api_methods_namespace_name_snapshotId
    """
    uri = "methods/{0}/{1}/{2}".format(namespace, name, snapshot_id)
    return __delete(uri)

def delete_repository_config(namespace, name, snapshot_id):
    """Redacts a configuration and all of its associated configurations.

    The configuration should exist in the methods repository.

    Args:
        namespace (str): configuration namespace
        configuration (str): configuration name
        snapshot_id (int): snapshot_id of the configuration

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/delete_api_configurations_namespace_name_snapshotId
    """
    uri = "configurations/{0}/{1}/{2}".format(namespace, name, snapshot_id)
    return __delete(uri)

def get_repository_method_acl(namespace, method, snapshot_id):
    """Get permissions for a method.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        version (int): snapshot_id of the method

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/getMethodACL
    """
    uri = "methods/{0}/{1}/{2}/permissions".format(namespace,method,snapshot_id)
    return __get(uri)

def update_repository_method_acl(namespace, method, snapshot_id, acl_updates):
    """Set method permissions.

    The method should exist in the methods repository.

    Args:
        namespace (str): Methods namespace
        method (str): method name
        snapshot_id (int): snapshot_id of the method
        acl_updates (list(dict)): List of access control updates

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/setMethodACL
    """

    uri = "methods/{0}/{1}/{2}/permissions".format(namespace,method,snapshot_id)
    return __post(uri, json=acl_updates)

def get_repository_config_acl(namespace, config, snapshot_id):
    """Get configuration permissions.

    The configuration should exist in the methods repository.

    Args:
        namespace (str): Configuration namespace
        config (str): Configuration name
        snapshot_id (int): snapshot_id of the method

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/getConfigACL
    """
    uri = "configurations/{0}/{1}/{2}/permissions".format(namespace,
                                                    config, snapshot_id)
    return __get(uri)

def update_repository_config_acl(namespace, config, snapshot_id, acl_updates):
    """Set configuration permissions.

    The configuration should exist in the methods repository.

    Args:
        namespace (str): Configuration namespace
        config (str): Configuration name
        snapshot_id (int): snapshot_id of the method
        acl_updates (list(dict)): List of access control updates

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/setConfigACL
    """

    uri = "configurations/{0}/{1}/{2}/permissions".format(namespace,
                                                config, snapshot_id)
    return __post(uri, json=acl_updates)

def get_method_configurations(namespace, method_name):
    """Get method configurations.

    Given the namespace/name of a method, returns all configurations in
    the repository that reference that method.

    Args:
        namespace (str): Method namespace
        method_name (str): Method name

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/get_api_methods_namespace_name_configurations

    """

    uri = "methods/{0}/{1}/configurations".format(namespace, method_name)

    return __get(uri)

def get_api_methods_definitions():
    """List method definitions.

    List method definitions - i.e. unique namespace/name pairs -
    with counts of snapshots and associated configurations

    Swagger:
        https://api.firecloud.org/#!/Method_Repository/get_api_methods_definitions

    """

    uri = "methods/definitions"

    return __get(uri)


#################
### 1.4 Profile
#################

def list_billing_projects():
    """Get activation information for the logged-in user.

    Swagger:
        https://api.firecloud.org/#!/Profile/billing
    """
    return __get("profile/billing")

################
### 1.5 Status
################

def get_status():
    """Request the status of FireCloud services.

    Swagger:
        https://api.firecloud.org/#!/Status/status
    """
    root_url = fcconfig.root_url.rpartition("api")[0]
    return __get("status", root_url=root_url)

def health():
    """Health of FireCloud API.

    Swagger:
        https://api.firecloud.org/#!/Status/health
    """
    root_url = fcconfig.root_url.rpartition("api")[0]
    return __get("health", root_url=root_url)

######################
### 1.6 Submissions
######################

def list_submissions(namespace, workspace):
    """List submissions in FireCloud workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Submissions/listSubmissions
    """
    uri = "workspaces/{0}/{1}/submissions".format(namespace, workspace)
    return __get(uri)

def create_submission(wnamespace, workspace, cnamespace, config,
                      entity, etype, expression=None, use_callcache=True):
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

    Swagger:
        https://api.firecloud.org/#!/Submissions/createSubmission
    """

    uri = "workspaces/{0}/{1}/submissions".format(wnamespace, workspace)
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

def abort_submission(namespace, workspace, submission_id):
    """Abort running job in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier

    Swagger:
        https://api.firecloud.org/#!/Submissions/deleteSubmission
    """
    uri = "workspaces/{0}/{1}/submissions/{2}".format(namespace,
                                        workspace, submission_id)
    return __delete(uri)

def get_submission(namespace, workspace, submission_id):
    """Request submission information.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier

    Swagger:
        https://api.firecloud.org/#!/Submissions/monitorSubmission
    """
    uri = "workspaces/{0}/{1}/submissions/{2}".format(namespace,
                                            workspace, submission_id)
    return __get(uri)

def get_workflow_metadata(namespace, workspace, submission_id, workflow_id):
    """Request the metadata for a workflow in a submission.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        workflow_id (str): Workflow's unique identifier.

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowMetadata
    """
    uri = "workspaces/{0}/{1}/submissions/{2}/workflows/{3}".format(namespace,
                                            workspace, submission_id, workflow_id)
    return __get(uri)

def get_workflow_outputs(namespace, workspace, submission_id, workflow_id):
    """Request the outputs for a workflow in a submission.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        workflow_id (str): Workflow's unique identifier.

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowOutputsInSubmission
    """
    uri = "workspaces/{0}/{1}/".format(namespace, workspace)
    uri += "submissions/{0}/workflows/{1}/outputs".format(submission_id,
                                                            workflow_id)
    return __get(uri)

def get_submission_queue():
    """ List workflow counts by queueing state.

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowQueueStatus
    """
    return __get("submissions/queueStatus")

#####################
### 1.7 Workspaces
#####################

def list_workspaces():
    """Request list of FireCloud workspaces.

    Swagger:
        https://api.firecloud.org/#!/Workspaces/listWorkspaces
    """
    return __get("workspaces")

def create_workspace(namespace, name, authorizationDomain="", attributes=None):
    """Create a new FireCloud Workspace.

    Args:
        namespace (str): project to which workspace belongs
        name (str): Workspace name
        protected (bool): If True, this workspace is protected by dbGaP
            credentials. This option is only available if your FireCloud
            account is linked to your NIH account.
        attributes (dict): Workspace attributes as key value pairs

    Swagger:
        https://api.firecloud.org/#!/Workspaces/createWorkspace
    """

    if not attributes:
        attributes = dict()

    body = {
        "namespace": namespace,
        "name": name,
        "attributes": attributes
    }
    if authorizationDomain:
        authDomain = [{"membersGroupName": authorizationDomain}]
    else:
        authDomain = []

    body["authorizationDomain"] = authDomain

    return __post("workspaces", json=body)

def delete_workspace(namespace, workspace):
    """Delete FireCloud Workspace.

    Note: This action is not reversible. Be careful!

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Workspaces/deleteWorkspace
    """
    uri = "workspaces/{0}/{1}".format(namespace, workspace)
    return __delete(uri)

def get_workspace(namespace, workspace):
    """Request FireCloud Workspace information.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Workspaces/getWorkspace
    """
    uri = "workspaces/{0}/{1}".format(namespace, workspace)
    return __get(uri)

def get_workspace_acl(namespace, workspace):
    """Request FireCloud access aontrol list for workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Workspaces/getWorkspaceAcl
    """
    uri = "workspaces/{0}/{1}/acl".format(namespace, workspace)
    return __get(uri)

def update_workspace_acl(namespace, workspace, acl_updates, invite_users_not_found=False):
    """Update workspace access control list.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        acl_updates (list(dict)): Acl updates as dicts with two keys:
            "email" - Firecloud user email
            "accessLevel" - one of "OWNER", "READER", "WRITER", "NO ACCESS"
            Example: {"email":"user1@mail.com", "accessLevel":"WRITER"}
        invite_users_not_found (bool): true to invite unregistered users, false to ignore

    Swagger:
        https://api.firecloud.org/#!/Workspaces/updateWorkspaceACL
    """
    uri = "{0}workspaces/{1}/{2}/acl?inviteUsersNotFound={3}".format(fcconfig.root_url,
                                                namespace, workspace, str(invite_users_not_found).lower())
    headers = _fiss_agent_header({"Content-type":  "application/json"})
    # FIXME: create __patch method, akin to __get, __delete etc
    return __SESSION.patch(uri, headers=headers, data=json.dumps(acl_updates))

def clone_workspace(from_namespace, from_workspace, to_namespace, to_workspace,
                    authorizationDomain=""):
    """Clone a FireCloud workspace.

    A clone is a shallow copy of a FireCloud workspace, enabling
    easy sharing of data, such as TCGA data, without duplication.

    Args:
        from_namespace (str):  project (namespace) to which source workspace belongs
        from_workspace (str): Source workspace's name
        to_namespace (str):  project to which target workspace belongs
        to_workspace (str): Target workspace's name
        authorizationDomain: (str) required authorization domains

    Swagger:
        https://api.firecloud.org/#!/Workspaces/cloneWorkspace
    """

    if authorizationDomain:
        if isinstance(authorizationDomain, string_types):
            authDomain = [{"membersGroupName": authorizationDomain}]
        else:
            authDomain = [{"membersGroupName": authDomain} for authDomain in authorizationDomain]
    else:
        authDomain = []

    body = {
        "namespace": to_namespace,
        "name": to_workspace,
        "attributes": dict(),
        "authorizationDomain": authDomain,
    }

    uri = "workspaces/{0}/{1}/clone".format(from_namespace, from_workspace)
    return __post(uri, json=body)

def lock_workspace(namespace, workspace):
    """Lock FireCloud workspace, making it read-only.

    This prevents modifying attributes or submitting workflows
    in the workspace. Can be undone with unlock_workspace()

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Workspaces/lockWorkspace
    """
    uri = "workspaces/{0}/{1}/lock".format(namespace, workspace)
    return __put(uri)

def unlock_workspace(namespace, workspace):
    """Unlock FireCloud workspace.

    Enables modifications to a workspace. See lock_workspace()

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name

    Swagger:
        https://api.firecloud.org/#!/Workspaces/unlockWorkspace
    """
    uri = "workspaces/{0}/{1}/unlock".format(namespace, workspace)
    return __put(uri)

def update_workspace_attributes(namespace, workspace, attrs):
    """Update or remove workspace attributes.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        attrs (list(dict)): List of update operations for workspace attributes.
            Use the helper dictionary construction functions to create these:

            _attr_set()      : Set/Update attribute
            _attr_rem()     : Remove attribute
            _attr_ladd()    : Add list member to attribute
            _attr_lrem()    : Remove list member from attribute

    Swagger:
        https://api.firecloud.org/#!/Workspaces/updateAttributes
    """
    headers = _fiss_agent_header({"Content-type":  "application/json"})
    uri = "{0}workspaces/{1}/{2}/updateAttributes".format(fcconfig.root_url,
                                                        namespace, workspace)
    body = json.dumps(attrs)

    # FIXME: create __patch method, akin to __get, __delete etc
    return __SESSION.patch(uri, headers=headers, data=body)

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

#####################
### 1.8 Groups
#####################

def get_groups():
    """Get the list of the groups that the caller is a member of
    
    Swagger:
        https://api.firecloud.org/#!/Groups/getGroups
    """
    return __get('groups')

def get_group(group):
    """View the members in a group (must be an Admin of the group)

    Args:
        group (str): Group name
    
    Swagger:
        https://api.firecloud.org/#!/Groups/getGroup
    """
    uri = "groups/{0}".format(group)
    return __get(uri)

def delete_group(group):
    """Delete a group that the caller owns

    Args:
        group (str): Group name
    
    Swagger:
        https://api.firecloud.org/#!/Groups/deleteGroup
    """
    uri = "groups/{0}".format(group)
    return __delete(uri)

def create_group(group):
    """Create a new group

    Args:
        group (str): Group name
    
    Swagger:
        https://api.firecloud.org/#!/Groups/createGroup
    """
    uri = "groups/{0}".format(group)
    return __post(uri)

def add_user_to_group(group, role, email):
    """Add a user to a group the caller owns

    Args:
        group (str): Group name
        role (str) : Role of user for group; either 'member' or 'admin'
        email (str): Email of user or group to add
    
    Swagger:
        https://api.firecloud.org/#!/Groups/addUserToGroup
    """
    uri = "groups/{0}/{1}/{2}".format(group, role, email)
    return __put(uri)

def remove_user_from_group(group, role, email):
    """Remove a user from a group the caller owns

    Args:
        group (str): Group name
        role (str) : Role of user for group; either 'member' or 'admin'
        email (str): Email of user or group to remove
    
    Swagger:
        https://api.firecloud.org/#!/Groups/removeUserFromGroup
    """
    uri = "groups/{0}/{1}/{2}".format(group, role, email)
    return __delete(uri)

def request_access_to_group(group):
    """Request access to a group

    Args:
        group (str): Group name
    
    Swagger:
        https://api.firecloud.org/#!/Groups/requestAccessToGroup
    """
    uri = "groups/{0}/requestAccess".format(group)
    return __post(uri)
