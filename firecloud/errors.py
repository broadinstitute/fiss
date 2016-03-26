class FirecloudServerError(RuntimeError):
    """
    Indicates a server error, i.e a 4XX or 5XX HTTP response

    400 - Bad request
    403 - Forbidden

    """
    def __init__(self, code, message):
        msg = "FireCloud Server Error: " + str(code) + " " + message
        RuntimeError.__init__(self, msg)

