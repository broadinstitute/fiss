#! /usr/bin/env python

from firecloud import api as fapi
from firecloud.errors import FireCloudServerError
from firecloud.entity import Entity
import json
import os

class Workspace(object):
    """
    Class reperesentation of a FireCloud Workspace
    """

    def __init__(self, namespace, name, api_url=fapi.PROD_API_ROOT):
        """
        Get an existing workspace from Firecloud by name. 

        Raises ValueError if the workspace does not exist.
        Raises FireCloudServerError if request receives a 500.
        """
        self.api_url = api_url
        self.namespace = namespace
        self.name = name

        ## Call out to FireCloud
        r, c = fapi.get_workspace(namespace, name, api_url)

        if r.status == 200:
            # Parse the json response
            self.data = json.loads(c)
        elif r.status == 404:
            emsg = "Workspace " + namespace + "/" + name + " does not exist"
            raise FireCloudServerError(r.status, emsg)
        elif r.status == 500:
            raise FireCloudServerError(r.status, "Internal Server Error")

    @staticmethod
    def new(namespace, name, protected=False, 
            attributes=dict(), api_url=fapi.PROD_API_ROOT):
        """
        Create a new workspace on firecloud and return a Workspace Object
        """
        r, c = fapi.create_workspace(namespace, name, protected, attributes, api_url)
        fapi._check_response(r, c, [201])
        return Workspace(namespace, name, api_url)

    def refresh(self):
        """
        Reload workspace data from firecloud. Workspace data is cached into 
        self.data, and may become stale
        """
        r, c = fapi.get_workspace(self.namespace, self.name, self.api_url)
        fapi._check_response(r, c, [200])
        self.data = json.loads(c)
        return self

    def delete(self):
        """
        Delete the workspace from FireCloud. Be careful!
        """
        r, c = fapi.delete_workspace(self.namespace, self.name)
        fapi._check_response(r, c, [202])

    # Getting useful information out of the bucket
    def json(self):
        """
        Get a JSON representation of the bucket
        """
        return str(json.dumps(self.data))

    def bucket(self):
        """
        Google bucket id for this workspace
        """
        return str(self.data["workspace"]["bucketName"])

    def lock(self):
        r, c = fapi.lock_workspace(self.namespace, self.name, self.api_url)
        fapi._check_response(r, c, [204])
        self.data['workspace']['isLocked'] = True
        return self

    def unlock(self):
        r, c = fapi.unlock_workspace(self.namespace, self.name, self.api_url)
        fapi._check_response(r, c, [204])
        self.data['workspace']['isLocked'] = False
        return self

    def attributes(self):
        """
        Get a dictionary of workspace attributes
        """
        return self.data["workspace"]["attributes"]

    def get_attribute(self, attr):
        """
        Get value of workspace attribute
        """
        return self.data["workspace"]["attributes"].get(attr, None)

    def update_attribute(self, attr, value):
        update = [fapi._attr_up(attr, value)]
        r, c = fapi.update_workspace_attributes(self.namespace, self.name,
                                                update, self.api_url)
        fapi._check_response(r, c, [200])

    def remove_attribute(self, attr):
        update = [fapi._attr_rem(attr)]
        r, c = fapi.update_workspace_attributes(self.namespace, self.name,
                                                update, self.api_url)
        fapi._check_response(r, c, [200])

    def import_tsv(self, tsv_file):
        """
        Upload entities by providing a tsv import file.
        """
        r, c = fapi.upload_entities_tsv(self.namespace, self.name,
                                        self.tsv_file, self.api_url)
        fapi._check_response(r, c, [200, 201])

    def get_entity(self, etype, entity_id):
        """
        Get an entity by type & id
        """
        r, c = fapi.get_entity(self.namespace, self.name, etype,
                               entity_id, self.api_url)
        fapi._check_response(r, c, [200])
        dresp = json.loads(c)
        return Entity(etype, entity_id, dresp['attributes'])


    def import_entities(self, entities):
        """
        Import participant entities
        """
        edata = Entity.create_payload(entities)
        r, c = fapi.upload_entities(self.namespace, self.name, 
                                    edata, self.api_url)
        fapi._check_response(r, c, [200, 201])

    def create_set(self, set_id, etype, entities):
        """
        Create a set of entities and upload to FireCloud
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


        r, c = fapi.upload_entities(self.namespace, self.name,
                                    payload, self.api_url)
        fapi._check_response(r, c, [200, 201])

    def create_sample_set(self, sset_id, samples):
        return self.create_set(sset_id, "sample", samples)

    def create_pair_set(self, pset_id, pairs):
        return self.create_set(pset_id, "pair", pairs)

    def create_participant_set(self, pset_id, participants):
        return self.create_set(pset_id, "participant", participants)


