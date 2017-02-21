#! /usr/bin/env python

import unittest
import time
import json
import logging
from getpass import getuser


import nose
from six import print_

from firecloud.fiss import main as call_fiss
from firecloud import api as fapi

# Context manager to capture stdout when calling another function
# Source: http://stackoverflow.com/questions/16571150/how-to-capture-stdout-output-from-a-python-function-call
from cStringIO import StringIO
import sys

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout


class TestFISS(unittest.TestCase):
    """Unit test the firecloud.api module.

    There should be at least one test per api call,
    with composite tests as feasible.
    """
    @classmethod
    def setUpClass(cls):
        """Set up workspaces to run tests against conditions."""
        # Username of person running the tests
        cls.user = getuser()

        # get a list of available namespaces

        r = fapi.list_billing_projects()
        projs = [d['projectName'] for d in r.json()]

        # prefer projects names with 'test' in the name, if present
        test_projs = sorted([p for p in projs if 'test' in p])

        if len(test_projs) > 0:
            cls.namespace = test_projs[0]
        elif len(projs) > 0:
            cls.namespace = projs[0]
        else:
            raise ValueError("ERROR: You do not have access to any firecloud"
                             " billing accounts, aborting tests")

        print_("Running tests using namespace: " + cls.namespace)

        # Set up a static workspace that will exist for the duration
        # of the tests. Individual workspaces will be created as temp,
        # but are responsible for tearing themselves down
        cls.static_workspace = cls.user + '_FISS_CLI_UNITTEST'

        fapi.create_workspace(cls.namespace, cls.static_workspace)

    @classmethod
    def tearDownClass(cls):
        """Tear down test conditions."""
        # Delete the static workspace
        fapi.delete_workspace(cls.namespace, cls.static_workspace)


    def test_ping(self):
        """Test fissfc ping"""
        ret = call_fiss(["fissfc", "ping"])
        self.assertEqual(0, ret)

    def test_space_info(self):
        """Test fissfc space_info"""
        fargs = [ "fissfc", "space_info",
                  "-p", self.namespace,
                  "-w", self.static_workspace
                ]
        with Capturing() as fiss_output:
            ret = call_fiss(fargs)
        logging.debug(fiss_output)
        space_info = json.loads(''.join(fiss_output))
        self.assertEqual(space_info['workspace']['name'], self.static_workspace)
        self.assertEqual(0, ret)

    def test_dash_l(self):
        """Test fissfc -l"""
        with Capturing() as fiss_output:
            ret = call_fiss(["fissfc", "-l"])
        fiss_output = ''.join(fiss_output)
        logging.debug(fiss_output)
        self.assertIn("space_info", fiss_output)

        # Also test -l <pattern>
        with Capturing() as fiss_output2:
            ret = call_fiss(["fissfc", "-l", "config"])
        fo2 = ''.join(fiss_output2)
        logging.debug(fo2)
        self.assertIn("config_validate", fo2)
        self.assertNotIn("space_info", fo2)

    def test_dash_F(self):
        """Test fissfc -F"""
        with Capturing() as fiss_output:
            call_fiss(["fissfc", "-F", "space_info"])
        fo = ''.join(fiss_output)
        logging.debug(fo)
        self.assertIn('def space_info(args)', fo)

    def test_space_new_delete(self):
        """ Test fissfc space_new + space_delete """
        new_args = ["fissfc", "space_new", "-p", self.namespace, "-w", self.static_workspace + "_snd"]

        with Capturing() as new_output:
            ret = call_fiss(new_args)
        logging.debug(''.join(new_output))
        self.assertEqual(0, ret)

        del_args = ["fissfc", "-y", "space_delete", "-p", self.namespace, "-w", self.static_workspace + "_snd"]
        with Capturing() as del_output:
            ret = call_fiss(del_args)
        logging.debug(''.join(del_output))
        self.assertEqual(0, ret)

    def test_space_lock_unlock(self):
        """ Test fissfc space_lock + space_unlock """
        lock_args = ["fissfc", "space_lock", "-p", self.namespace, "-w", self.static_workspace]

        with Capturing() as lock_output:
            ret = call_fiss(lock_args)
        logging.debug(''.join(lock_output))
        self.assertEqual(0, ret)

        unlock_args = ["fissfc", "space_unlock", "-p", self.namespace, "-w", self.static_workspace]
        with Capturing() as unlock_output:
            ret = call_fiss(unlock_args)
        logging.debug(''.join(unlock_output))
        self.assertEqual(0, ret)




def main():
    nose.main()

if __name__ == '__main__':
    main()
