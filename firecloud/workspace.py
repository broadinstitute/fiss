import json
import os

from firecloud import api as fapi
from firecloud.errors import FireCloudServerError
from firecloud.entity import Entity

class Workspace(object):
    """A FireCloud Workspace.

    Attributes:
        api_url (str): API root used to interact with FireCloud,
            normally https://api.firecloud.org/api
        namespace (str): Google project for this workspace
        name (str): Workspace name
    """

    def __init__(self, namespace, name, api_url=fapi.PROD_API_ROOT):
        """Get an existing workspace from Firecloud by name.

        This method assumes that a workspace with the given name and
        namespace is present at the api_url given, and raises an error
        if it does not exist. To create a new workspace, use
        Workspace.new()

        Raises:
            FireCloudServerError:  Workspace does not exist, or
                API call fails
        """
        self.api_url = api_url
        self.namespace = namespace
        self.name = name

        ## Call out to FireCloud
        r = fapi.get_workspace(namespace, name, api_url)

        fapi._check_response_code(r, 200)
        self.data = r.json()


    @staticmethod
    def new(namespace, name, protected=False,
            attributes=dict(), api_url=fapi.PROD_API_ROOT):
        """Create a new FireCloud workspace.

        Returns:
            Workspace: A new FireCloud workspace

        Raises:
            FireCloudServerError: API call failed.
        """
        r = fapi.create_workspace(namespace, name, protected, attributes, api_url)
        fapi._check_response_code(r, 201)
        return Workspace(namespace, name, api_url)

    def refresh(self):
        """Reload workspace metadata from firecloud.

        Workspace metadata is cached in the data attribute of a Workspace,
        and may become stale, requiring a refresh().
        """
        r = fapi.get_workspace(self.namespace, self.name, self.api_url)
        fapi._check_response_code(r, 200)
        self.data = r.json()
        return self

    def delete(self):
        """Delete the workspace from FireCloud.

        Note:
            This action cannot be undone. Be careful!
        """
        r = fapi.delete_workspace(self.namespace, self.name)
        fapi._check_response_code(r, 202)

    # Getting useful information out of the bucket
    def __str__(self):
        """Return a JSON representation of the bucket."""
        return json.dumps(self.data, indent=2)

    def bucket(self):
        """Return google bucket id for this workspace."""
        return str(self.data["workspace"]["bucketName"])

    def lock(self):
        """Lock this Workspace.

        This causes the workspace to behave in a read-only way,
        regardless of access permissions.
        """
        r = fapi.lock_workspace(self.namespace, self.name, self.api_url)
        fapi._check_response_code(r, 204)
        self.data['workspace']['isLocked'] = True
        return self

    def unlock(self):
        """Unlock this Workspace."""
        r = fapi.unlock_workspace(self.namespace, self.name, self.api_url)
        fapi._check_response_code(r, 204)
        self.data['workspace']['isLocked'] = False
        return self

    def attributes(self):
        """Return a dictionary of workspace attributes"""
        return self.data["workspace"]["attributes"]

    def get_attribute(self, attr):
        """Return value of workspace attribute.

        If the attribute does not exist, return None
        """
        return self.data["workspace"]["attributes"].get(attr, None)

    def update_attribute(self, attr, value):
        """Set the value of a workspace attribute."""
        update = [fapi._attr_up(attr, value)]
        r = fapi.update_workspace_attributes(self.namespace, self.name,
                                             update, self.api_url)
        fapi._check_response_code(r, 200)

    def remove_attribute(self, attr):
        """Remove attribute from a workspace.

        Args:
            attr (str): attribute name
        """
        update = [fapi._attr_rem(attr)]
        r = fapi.update_workspace_attributes(self.namespace, self.name,
                                                update, self.api_url)
        self.data["workspace"]["attributes"].pop(attr, None)
        fapi._check_response_code(r, 200)

    def import_tsv(self, tsv_file):
        """Upload entity data to workspace from tsv loadfile.

        Args:
            tsv_file (file): Tab-delimited file of entity data
        """
        r = fapi.upload_entities_tsv(self.namespace, self.name,
                                     self.tsv_file, self.api_url)
        fapi._check_response_code(r, 201)

    def get_entity(self, etype, entity_id):
        """Return entity in this workspace.

        Args:
            etype (str): Entity type
            entity_id (str): Entity name/unique id
        """
        r = fapi.get_entity(self.namespace, self.name, etype,
                               entity_id, self.api_url)
        fapi._check_response_code(r, 200)
        dresp = r.json()
        return Entity(etype, entity_id, dresp['attributes'])

    def delete_entity(self, etype, entity_id):
        """Delete an entity in this workspace.

        Args:
            etype (str): Entity type
            entity_id (str): Entity name/unique id
        """
        r = fapi.delete_entity(self.namespace, self.name, etype,
                                  entity_id, self.api_url)
        fapi._check_response_code(r, 202)

    def import_entities(self, entities):
        """Upload entity objects.

        Args:
            entities: iterable of firecloud.Entity objects.
        """
        edata = Entity.create_payload(entities)
        r = fapi.upload_entities(self.namespace, self.name,
                                 edata, self.api_url)
        fapi._check_response_code(r, 201)

    def create_set(self, set_id, etype, entities):
        """Create a set of entities and upload to FireCloud.

        Args
            etype (str): one of {"sample, "pair", "participant"}
            entities: iterable of firecloud.Entity objects.
        """
        if etype not in {"sample", "pair", "participant"}:
            raise ValueError("Unsupported entity type:" + str(etype))

        payload = "membership:" + etype + "_set_id\t" + etype + "_id\n"

        for e in entities:
            if e.etype != etype:
                msg =  "Entity type '" + e.etype + "' does not match "
                msg += "set type '" + etype + "'"
                raise ValueError(msg)
            payload += set_id + '\t' + e.entity_id + '\n'


        r = fapi.upload_entities(self.namespace, self.name,
                                    payload, self.api_url)
        fapi._check_response_code(r, 201)

    def create_sample_set(self, sset_id, samples):
        """Create FireCloud sample_set"""
        return self.create_set(sset_id, "sample", samples)

    def create_pair_set(self, pset_id, pairs):
        """Create FireCloud pair_set"""
        return self.create_set(pset_id, "pair", pairs)

    def create_participant_set(self, pset_id, participants):
        """Create FireCloud participant_set"""
        return self.create_set(pset_id, "participant", participants)

    def submissions(self):
        """List job submissions in workspace."""
        r = fapi.get_submissions(self.namespace, self.name, self.api_url)
        fapi._check_response_code(r, 200)
        return r.json()

    def entity_types(self):
        """List entity types in workspace."""
        r = fapi.get_entity_types(self.namespace, self.name, self.api_url)
        fapi._check_response_code(r, 200)
        return r.json().keys()

    def entities(self):
        """List all entities in workspace."""
        r = fapi.get_entities_with_type(self.namespace,
                                        self.name, self.api_url)
        fapi._check_response_code(r, 200)
        edicts = r.json()
        return [Entity(e['entityType'], e['name'], e['attributes'])
                for e in edicts]

    def __get_entities(self, etype):
        """Helper to get entities for a given type."""
        r = fapi.get_entities(self.namespace, self.name,
                              etype, self.api_url)
        fapi._check_response_code(r, 200)
        return [Entity(e['entityType'], e['name'], e['attributes'])
                for e in r.json()]

    def samples(self):
        """List samples in a workspace."""
        return self.__get_entities("sample")

    def participants(self):
        """List participants in a workspace."""
        return self.__get_entities("participant")

    def pairs(self):
        """List pairs in a workspace."""
        return self.__get_entities("pair")

    def sample_sets(self):
        """List sample sets in a workspace."""
        return self.__get_entities("sample_set")

    def participant_sets(self):
        """List participant sets in a workspace."""
        return self.__get_entities("participant_set")

    def pair_sets(self):
        """List pair sets in a workspace."""
        return self.__get_entities("pair_set")

    def copy_entities(self, from_namespace, from_workspace, etype, enames):
        """Copy entities from another workspace.

        Args:
            from_namespace (str): Source workspace namespace
            from_workspace (str): Source workspace name
            etype (str): Entity type
            enames (list(str)): List of entity names to copy
        """
        r = fapi.copy_entities(from_namespace, from_workspace,
                               self.namespace, self.name, etype, enames,
                               self.api_url)
        fapi._check_response_code(r, 201)

    def configs(self):
        """Get method configurations in a workspace."""
        raise NotImplementedError
        r = fapi.get_configs(self.namespace, self.name, self.api_url)
        fapi._check_response_code(r, 200)
        cdata = r.json()
        configs = []
        for c in cdata:
            cnamespace = c['namespace']
            cname = c['name']
            root_etype = c['rootEntityType']
            method_namespace = c['methodRepoMethod']['methodNamespace']
            method_name = c['methodRepoMethod']['methodName']
            method_version = c['methodRepoMethod']['methodVersion']

    def acl(self):
        """Get the access control list for this workspace."""
        r = fapi.get_workspace_acl(self.namespace, self.name, self.api_url)
        fapi._check_response_code(r, 200)
        return r.json()

    def set_acl(self, role, users):
        """Set access permissions for this workspace

        Args:
            role (str): Access level
                one of {one of "OWNER", "READER", "WRITER", "NO ACCESS"}
            users (list(str)): List of users to give role to
        """
        acl_updates = [{"email": user, "accessLevel": role} for user in users]
        r = fapi.update_workspace_acl(self.namespace, self.name,
                                      acl_updates, self.api_url)
        fapi._check_response_code(r, 200)

    def clone(self, to_namespace, to_name):
        """Clone this workspace.

        Args:
            to_namespace (str): Target workspace namespace
            to_name (str): Target workspace name
        """
        r = fapi.clone_workspace(self.namespace, self.name,
                                 to_namespace, to_name, self.api_url)
        fapi._check_response_code(r, 201)
        return Workspace(to_namespace, to_name, self.api_url)
