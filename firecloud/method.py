import firecloud.api as fapi
import json
from firecloud.errors import FireCloudServerError
from os.path import isfile

class Method(object):
    """A FireCloud Method.

    Attributes:
        namespace (str): Method namespace for this method
        name (str): Method name
        snapshot_id (int): Version number
        wdl (str): WDL description 
        synopsis (str): Short description of task
        documentation (str): Extra documentation for method
        api_url (str): FireCloud API root
    """

    def __init__(self, namespace, name, 
                 snapshot_id, api_url=fapi.PROD_API_ROOT):
        r, c = fapi.get_method(namespace, name, snapshot_id, api_url)
        if r.status != 200:
            raise FireCloudServerError(r.status, c)
        data = json.loads(c)
        self.namespace = namespace
        self.name = name
        self.snapshot_id = int(data["snapshotId"])
        self.wdl = data["payload"]
        self.synopsis = data["synopsis"]
        self.documentation = data["documentation"]
        self.api_url = api_url


    @staticmethod
    def new(namespace, name, wdl, synopsis, 
            documentation=None, api_url=fapi.PROD_API_ROOT):
        """Create new FireCloud method.

        If the namespace + name already exists, a new snapshot is created.

        Args:
            namespace (str): Method namespace for this method
            name (str): Method name
            wdl (file): WDL description 
            synopsis (str): Short description of task
            documentation (file): Extra documentation for method
        """
        r, c = fapi.update_workflow(namespace, name, synopsis,
                                    wdl, documentation, api_url)
        fapi._check_response(r, c, [201])
        d = json.loads(c)
        return Method(namespace, name, d["snapshotId"])

    def template(self):
        """Return a method template for this method."""
        r, c = fapi.get_config_template(self.namespace, self.name,
                                        self.snapshot_id, self.api_url)
        fapi._check_response(r, c, [200])
        return json.loads(c)

    def inputs_outputs(self):
        """Get information on method inputs & outputs."""
        r, c = fapi.get_inputs_outputs(self.namespace, self.name,
                                       self.snapshot_id, self.api_url)
        fapi._check_response(r, c, [200])
        return json.loads(c)

    def permissions(self):
        """Get the access control list for this method."""
        r, c = fapi.get_repository_method_acl(
            self.namespace, self.name, self.snapshot_id, self.api_url)
        fapi._check_response(r, c, [200])
        return json.loads(c)

    def set_acl(self, role, users):
        """Set permissions for this method.

        Args:
            role (str): Access level 
                one of {one of "OWNER", "READER", "WRITER", "NO ACCESS"}
            users (list(str)): List of users to give role to
        """
        acl_updates = [{"user": user, "role": role} for user in users]
        r, c = fapi.update_repository_method_acl(
            self.namespace, self.name, self.snapshot_id,
            acl_updates, self.api_url
        )
        fapi._check_response(r, c, [200])

