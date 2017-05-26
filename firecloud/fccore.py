#!/usr/bin/env python
# encoding: utf-8

# Front Matter {{{
'''
Copyright (c) 2017 The Broad Institute, Inc.  All rights are reserved.

confIg.py: this file is part of FISSFC.  See the <root>/COPYRIGHT
file for the SOFTWARE COPYRIGHT and WARRANTY NOTICE.

@author: Michael S. Noble
@date:  2017_05_17
'''

# }}}

from __future__ import print_function
import sys
import os
import configparser
from firecloud import __about__
from io import IOBase


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

__host = os.uname()[1].split('.')[0]
__fcconfig = attrdict({
    'api_url'    : 'https://api.firecloud.org/api',
    'user_agent' : "FISS/" + __about__.__version__,
    'debug'      : False,
    'page_size'  : 1000,
    'project'    : '',
    'workspace'  : '',
    'method_ns'  : '',
})

def fc_config_get(name):
    return __fcconfig[name]     # Returns default value if name is undefined

def fc_config_get_all():
    return attrdict(__fcconfig)

def fc_config_set(name, value):
    # FIXME: should validate critical variables, e.g. that type is not changed
    __fcconfig[name] = value

def fc_config_parse(config=None, *files):
    '''
    Read initial configuration state, from named config files; store
    this state within a config dictionary (which may be nested) whose keys may
    also be referenced as attributes (safely, defaulting to None if unset).  A
    config object may be passed in, as a way of accumulating or overwriting
    configuration state; if one is NOT passed, the default config obj is used
    '''

    if not config:
        config = __fcconfig
    cfgparser = configparser.SafeConfigParser()

    fileNames = list()
    for f in files:
        if isinstance(f, IOBase):
            f = f.name
        fileNames.append(f)

    # Give personal/user configuration the last say
    fileNames.append(os.path.expanduser('~/.fissconfig'))

    cfgparser.read(fileNames)

    # [DEFAULT] defines common variables for interpolation/substitution in
    # other sections, and are stored at the root level of the config object
    for keyval in cfgparser.items('DEFAULT'):
        #print("fc_config_parse: adding config variable %s=%s" % (keyval[0], str(keyval[1])))
        __fcconfig[keyval[0]] = keyval[1]

    for section in cfgparser.sections():
        config[section] = attrdict()
        for option in cfgparser.options(section):
            # DEFAULT vars ALSO behave as though they were defined in every
            # section, but we purposely skip them here so that each section
            # reflects only the options explicitly defined in that section
            if not config[option]:
                config[section][option] = cfgparser.get(section, option)

    return config
