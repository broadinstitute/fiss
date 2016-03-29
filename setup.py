import os
from setuptools import setup, find_packages

VERSION="0.8.0"
_README           = os.path.join(os.path.dirname(__file__), 'README')
_LONG_DESCRIPTION = open(_README).read()

# Setup information
setup(
    name = 'firecloud',
    version = VERSION,
    packages = find_packages(),
    description = 'Firecloud API bindings and FISS CLI',
    author = 'Tim DeFreitas',
    author_email = 'timdef@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    entry_points = {
        'console_scripts': [
            'fissfc = firecloud.fiss:main',
            'fiss = firecloud.fiss:main'
        ]
    },
    test_suite = 'nose.collector',
    install_requires = [
        'httplib2',
        'oauth2client',
        'yapsy'
    ],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",

    ],

)