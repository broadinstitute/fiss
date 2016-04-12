import json

class FireCloudServerError(RuntimeError):
    """Exception indicating a server error

    Attributes:
        code (int): HTTP response code indicating error type, e.g.:
            400 - Bad request
            403 - Forbidden
            500 - Internal Server Error

        message (str): Response content, if present
    """
    def __init__(self, code, message):
        self.code = code
        self.message = message
        emsg = str(code) + ": " + str(self.message)
        RuntimeError.__init__(self, emsg)

