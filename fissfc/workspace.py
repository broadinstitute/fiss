#! /usr/bin/env python

from fissfc import firecloud_api as fapi
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
        r, c = fapi.get_workspace(namespace, name, api_url)

        if r.status == 200:
            # Parse the json response
            self.data = json.loads(c)
        elif r.status == 404:
            emsg = "Workspace " + namespace + "/" + name + " does not exist"
            raise ValueError(emsg)
        elif r.status == 500:
            # TODO: Replace with FC server error class
            raise RuntimeError("FireCloudSeverError")

    @classmethod
    def new(namespace, name, attributes=dict(), api_url=PROD_API_ROOT):
        """
        Create a new workspace on firecloud and return a Workspace Object
        """
        fapi.create_workspace(namespace, name, attributes, api_url)
        return Workspace(namespace, name, api_url)

    # Getting useful information out of the bucket
    def json(self):
        """
        Get a JSON representation of the bucket
        """
        return json.dumps(self.data)

    def bucket(self):
        """
        Google bucket id for this workspace
        """
        return self.data["workspace"]["bucketName"]

