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
    author = 'Broad Institute CGA Genome Data Analysis Center',
    author_email = 'gdac@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    url = 'https://github.com/broadinstitute/fiss',
    entry_points = {
        'console_scripts': [
            'fissfc = firecloud.fiss:main_as_cli'
            # Disable until fiss is formally deprecated
            # 'fiss = firecloud.fiss:main'
        ]
    },
    test_suite = 'nose.collector',
    install_requires = [
        'google-auth',
        'pydot',
        'requests',
        'six',
        'pylint==1.7.2'
    ],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",

    ],

)
