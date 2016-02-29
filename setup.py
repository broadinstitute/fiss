import os
from setuptools import setup, find_packages
from fissfc import firecloud_cli

_README           = os.path.join(os.path.dirname(__file__), 'README')
_LONG_DESCRIPTION = open(_README).read()

# Setup information
setup(
    name = 'fissfc',
    version = firecloud_cli.__version__,
    packages = find_packages(),
    description = 'Firecloud python API and CLI',
    author = 'Tim DeFreitas',
    author_email = 'timdef@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    entry_points = {'console_scripts': ['fissfc = fissfc.firecloud_cli:main']},
    test_suite = 'nose.collector',
    install_requires = [
        'httplib2',
        'oauth2client'
    ],

    )