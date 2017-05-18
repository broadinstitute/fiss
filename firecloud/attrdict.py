
class attrdict(dict):
    """ dict whose members can be accessed as attributes, and default value is
    transparently returned for undefined keys; this yields more natural syntax
    dict[key]/dict.key for all use cases, instead of dict.get(key, <default>)
    """

    def __init__(self, srcdict=None, default=None):
        if srcdict is None:
            srcdict = {}
        dict.__init__(self, srcdict)
        self.__dict__["__default__"] = default

    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            return self.__dict__["__default__"]

    def __getattr__(self, item):
        return self.__getitem__(item)

    def __setattr__(self, item, value):
        if item in self.__dict__:
            dict.__setattr__(self, item, value)
        else:
            self.__setitem__(item, value)
