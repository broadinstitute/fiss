import os
from setuptools import setup, find_packages

VERSION="0.6.2"
_README           = os.path.join(os.path.dirname(__file__), 'README')
_LONG_DESCRIPTION = open(_README).read()

# Setup information
setup(
    name = 'fissfc',
    version = VERSION,
    packages = find_packages(),
    description = 'Firecloud python API and CLI',
    author = 'Tim DeFreitas',
    author_email = 'timdef@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    entry_points = {'console_scripts': ['fissfc = fissfc.firecloud_cli:main']},
    test_suite = 'nose.collector',
    install_requires = [
        'httplib2',
        'oauth2client',
        'yapsy'
    ],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3"
    ],

)