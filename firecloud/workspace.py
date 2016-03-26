#! /usr/bin/env python

from firecloud import api as fapi
from firecloud.errors import *
import json

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

    @classmethod
    def new(namespace, name, protected=False, 
            attributes=dict(), api_url=fapi.PROD_API_ROOT):
        """
        Create a new workspace on firecloud and return a Workspace Object
        """
        r, c = fapi.create_workspace(namespace, name, protected, attributes, api_url)
        if r.status != 201:
            raise FireCloudServerError(r.status, c)
        return Workspace(namespace, name, api_url)

    def refresh(self):
        """
        Reload workspace data from firecloud. Workspace data is cached into 
        self.data, and may become stale
        """
        r, c = fapi.get_workspace(self.namespace, self.name, self.api_url)
        if r.status != 200:
            raise FireCloudServerError(r.status, c)
        self.data = json.loads(c)
        return self

    def delete(self):
        """
        Delete the workspace from FireCloud. Be careful!
        """
        r, c = fapi.delete_workspace(self.namespace, self.name)
        if r.status != 202:
            raise FireCloudServerError(r.status, c)

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
        if r.status != 204:
            raise FireCloudServerError(r.status, c)
        self.data['workspace']['isLocked'] = True
        return self

    def unlock(self):
        r, c = fapi.unlock_workspace(self.namespace, self.name, self.api_url)
        if r.status != 204:
            raise FireCloudServerError(r.status, c)
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
        if r.status != 200:
            raise FireCloudServerError(r.status, c)

    def remove_attribute(self, attr):
        update = [fapi._attr_rem(attr)]
        r, c = fapi.update_workspace_attributes(self.namespace, self.name,
                                                update, self.api_url)
        if r.status != 200:
            raise FireCloudServerError(r.status, c)
