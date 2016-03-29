import firecloud.api as fapi
import json
from firecloud.errors import FireCloudServerError

class Config(object):
    """A FireCloud method configuration

    Attributes:
        namespace (str): Configuration namespace
        name (str): Configuration name
        snapshot_id (int): Version number
        api_url (str): FireCloud API root
    """

    def __init__(self, namespace, name, 
                 snapshot_id, api_url=fapi.PROD_API_ROOT):
        r, c = fapi.get_repository_configuration(
            namespace, config, snapshot_id, api_url)
        fapi._check_response(r, c, [200])
        self.namespace = namespace
        self.name = name
        self.snapshot_id = snapshot_id
        self.api_url = api_url

