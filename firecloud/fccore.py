#!/usr/bin/env python
# encoding: utf-8

# Front Matter {{{
'''
Copyright (c) 2017 The Broad Institute, Inc.  All rights are reserved.

fccore.py: this file is part of FISSFC.  See the <root>/COPYRIGHT
file for the SOFTWARE COPYRIGHT and WARRANTY NOTICE.

@author: Michael S. Noble
@date:  2017_05_17
'''

# }}}

from __future__ import print_function
import sys
import os
import configparser
import tempfile
import shutil
from collections import Iterable
import subprocess
from io import IOBase
from firecloud import __about__
from google.auth import environment_vars
from six import string_types

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

def config_get(name):
    return __fcconfig[name]     # Returns default value if name is undefined

def config_get_all():
    return __fcconfig

def config_set(name, value):
    # FIXME: should validate critical variables, e.g. that type is not changed
    __fcconfig[name] = value

def __set_credentials(credentials):
    previous_value = __fcconfig.credentials
    if not credentials:
        return previous_value
    if not os.path.isfile(credentials):
        print("\t\t__set_credentials: {0} is not an accessible file".format(credentials),file=sys.stderr)
        # simply keep previous value
    else:
        __fcconfig.credentials = credentials
        # Use custom credentials file for authentication
        os.environ[environment_vars.CREDENTIALS] = credentials
    return previous_value

def __set_verbosity(verbosity):
    previous_value = __fcconfig.verbosity
    try:
        __fcconfig.verbosity = int(verbosity)
    except Exception:
        print("\t\t__set_verbosity: caught exception type(verbosity)={0}".format(type(verbosity)),file=sys.stderr)
        pass                            # simply keep previous value
    return previous_value

def __get_verbosity():
    return __fcconfig.verbosity

def __set_root_url(url):
    previous_value = __fcconfig.root_url
    if not url:
        return previous_value
    try:
        if not url.endswith('/'):
            url += '/'
        __fcconfig.root_url = url
    except Exception:
        print("\t\t__set_root_url: caught exception type(url)={0}".format(type(url)),file=sys.stderr)
        pass                            # simply keep previous value
    return previous_value

__fcconfig = attrdict({
    'credentials'      : '',
    'root_url'         : 'https://api.firecloud.org/api/',
    'user_agent'       : 'FISS/' + __about__.__version__,
    'debug'            : False,
    'verbosity'        : 0,
    'page_size'        : 1000,
    'project'          : '',
    'workspace'        : '',
    'method_ns'        : '',
    'entity_type'      : 'sample_set',
    'get_verbosity'    : __get_verbosity,
    'set_verbosity'    : __set_verbosity,
    'set_credentials'  : __set_credentials,
    'set_root_url'     : __set_root_url
})

def config_parse(files=None, config=None, config_profile=".fissconfig", **kwargs):
    '''
    Read initial configuration state, from named config files; store
    this state within a config dictionary (which may be nested) whose keys may
    also be referenced as attributes (safely, defaulting to None if unset).  A
    config object may be passed in, as a way of accumulating or overwriting
    configuration state; if one is NOT passed, the default config obj is used
    '''
    local_config = config
    config = __fcconfig

    cfgparser = configparser.SafeConfigParser()

    filenames = list()

    # Give personal/user followed by current working directory configuration the first say
    filenames.append(os.path.join(os.path.expanduser('~'), config_profile))

    filenames.append(os.path.join(os.getcwd(), config_profile))

    if files:
        if isinstance(files, string_types):
            filenames.append(files)
        elif isinstance(files, Iterable):
            for f in files:
                if isinstance(f, IOBase):
                    f = f.name
                filenames.append(f)

    cfgparser.read(filenames)

    # [DEFAULT] defines common variables for interpolation/substitution in
    # other sections, and are stored at the root level of the config object
    for keyval in cfgparser.items('DEFAULT'):
        #print("config_parse: adding config variable %s=%s" % (keyval[0], str(keyval[1])))
        __fcconfig[keyval[0]] = keyval[1]

    for section in cfgparser.sections():
        config[section] = attrdict()
        for option in cfgparser.options(section):
            # DEFAULT vars ALSO behave as though they were defined in every
            # section, but we purposely skip them here so that each section
            # reflects only the options explicitly defined in that section
            if not config[option]:
                config[section][option] = cfgparser.get(section, option)

    config.verbosity = int(config.verbosity)
    if not config.root_url.endswith('/'):
        config.root_url += '/'
    if os.path.isfile(config.credentials):
        os.environ[environment_vars.CREDENTIALS] = config.credentials

    # if local_config override options with passed options
    if local_config is not None:
        for key, value in local_config.items():
            config[key] = value

    # if any explict config options are passed override.
    for key, value in kwargs.items():
        config[key] = value

    return config

# Text editing capability, loosely based on StackOverflow post: {{{
#           call up an EDITOR (vim) from a python script

__EDITOR__ = os.environ.get('EDITOR','vi')

def edit_text(text=None):
    # Edit block of text in a single string, returning the edited result
    tf = tempfile.NamedTemporaryFile(suffix=".tmp")
    if text:
        tf.write(text)
        tf.flush()
    subprocess.call([__EDITOR__, tf.name])
    with open(tf.name, 'r') as newfile:
        text = newfile.read()
    tf.close()
    try:
        # Attempt to clean hidden temp files that EDITOR may have created
        os.remove(tf.name + "~")
    except OSError:
        pass
    return text

def edit_file(name, backup=None):
    # Edit file in place, optionally backing up first
    # Returns True if file was modified else False
    if backup:
        shutil.copy2(name, backup)
    # Record time of last modification: tolm
    previous_tolm = os.stat(name).st_mtime
    subprocess.call([__EDITOR__, name])
    current_tolm  = os.stat(name).st_mtime
    return current_tolm != previous_tolm

# }}}
