import os
from setuptools import setup, find_packages
from firecloud.__about__ import __version__
_README           = os.path.join(os.path.dirname(__file__), 'README')
_LONG_DESCRIPTION = open(_README).read()

# Setup information
setup(
    name = 'firecloud',
    version = __version__,
    packages = find_packages(),
    description = 'Firecloud API bindings and FISS CLI',
    author = 'Tim DeFreitas',
    author_email = 'timdef@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    entry_points = {
        'console_scripts': [
            'fissfc = firecloud.fiss:main'
            # Disable until fiss is formally deprecated
            # 'fiss = firecloud.fiss:main'
        ]
    },
    test_suite = 'nose.collector',
    install_requires = [
        'requests',
        'oauth2client',
        'yapsy',
        'six'
    ],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",

    ],

)
