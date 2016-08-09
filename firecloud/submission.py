import json

import firecloud.api as fapi

class Submission(object):
    """A FireCloud method configuration

    Attributes:
        namespace (str): workspace namespace
        workspace (str): workspace name
        submission_id (int): Unique submission identifier
        api_url (str): FireCloud API root
    """

    def __init__(self, namespace, workspace,
                 submission_id, api_url=fapi.PROD_API_ROOT):
        r = fapi.get_submission(namespace, workspace,
                                submission_id, api_url)
        fapi._check_response_code(r, 200)

        self.namespace = namespace
        self.workspace = workspace
        self.submission_id = submission_id
        self.api_url = api_url

    @staticmethod
    def new(wnamespace, workspace, cnamespace, config,
            entity_id, etype, expression, api_url=fapi.PROD_API_ROOT):
        r, c = fapi.create_submission(wnamespace, workspace, cnamespace,
                                      config, entity_id, expression,
                                      api_url)
    fapi._check_response_code(r, 201)
        #return Submission(wnamespace, workspace, ??)
