#! /usr/bin/env python

import unittest
import time
import json
import logging
import os
from getpass import getuser
import nose
from six import print_
from firecloud.fiss import main as call_fiss
from firecloud import api as fapi

# Context manager to capture stdout when calling another function
# Source: http://stackoverflow.com/questions/16571150/how-to-capture-stdout-output-from-a-python-function-call
# from cStringIO import StringIO
# replace cStringIO with StringIO for python3 compatibility
from io import StringIO
import sys
import ast

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        if sys.version_info[0] < 3:
            self.extend(self._stringio.getvalue().splitlines())
        else:
            self.extend(ast.literal_eval(self._stringio.getvalue()).decode().splitlines())
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

        print_("\nStarting high-level CLI tests ...\n", file=sys.stderr)
        # Username of person running the tests
        cls.user = getuser()

        fiss_verbosity = os.environ.get("FISS_TEST_VERBOSITY", None)
        if fiss_verbosity == None:
            fiss_verbosity = 0
        fapi.set_verbosity(fiss_verbosity)

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

        logging.debug("Running tests using namespace: " + cls.namespace)

        # Set up a static workspace that will exist for the duration of the
        # tests. Individual workspaces will be created as temp, but are
        # responsible for tearing themselves down.  And, just in case a
        # previous test failed, we attempt to delete before creating
        cls.static_workspace = cls.user + '_FISS_CLI_UNITTEST'
        r = fapi.delete_workspace(cls.namespace, cls.static_workspace)
        r = fapi.create_workspace(cls.namespace, cls.static_workspace)
        fapi._check_response_code(r, 201)
        sw = r.json()
        cls.sw = sw

    @classmethod
    def tearDownClass(cls):
        """Tear down test conditions."""
        # Delete all workspaces with _FISS_CLI_UNITTEST in the name
        workspaces = fapi.list_workspaces().json()

        test_spaces = [w['workspace']['name'] for w in workspaces if '_FISS_CLI_UNITTEST' in w['workspace']['name']]
        for ts in test_spaces:
            logging.debug("Deleting workspace: " + ts)
            fapi.delete_workspace(cls.namespace, ts)

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

        args = ["fissfc", "space_lock", "-p", self.namespace, "-w", self.static_workspace]
        with Capturing() as lock_output:
            ret = call_fiss(args)
        logging.debug(''.join(lock_output))
        self.assertEqual(0, ret)

        args[1] = "space_unlock"
        with Capturing() as unlock_output:
            ret = call_fiss(args)
        logging.debug(''.join(unlock_output))
        self.assertEqual(0, ret)

    def test_space_search(self):
        """Test space_search """
        ss_args = ["fissfc", "space_search", "-b", self.sw['bucketName']]
        with Capturing() as search_output:
            ret = call_fiss(ss_args)

        # We should get the static bucket back when searching using its bucket name
        self.assertIn(self.sw['name'], ''.join(search_output))
        self.assertEqual(0, ret)

    def test_space_list(self):
        """Test space_list """
        sl_args = ["fissfc", "space_list"]
        with Capturing() as sl:
            ret = call_fiss(sl_args)

        self.assertIn(self.static_workspace, ''.join(sl))
        self.assertEquals(0, ret)

    def test_space_exists(self):
        """Test space_exists"""
        args = ["fissfc", "space_exists",
                  "-p", self.namespace,
                  "-w", self.static_workspace
                ]
        with Capturing() as output:
            ret = call_fiss(args)

        self.assertIn(self.static_workspace, ''.join(output))
        self.assertEquals(0, ret)

    def test_entity_import(self):
        """Test entity_import """
        eia = ["fissfc", "entity_import", "-p", self.namespace, "-w", self.static_workspace,
               "-f", os.path.join("firecloud", "tests", "participants.tsv")]
        ret = call_fiss(eia)
        self.assertEquals(0, ret)

    def test_attr_workspace(self):
        """Test attr_get/set on workspace """
        call_fiss(["fissfc", "-y", "attr_set", "-p", self.namespace, "-w", self.static_workspace,
                   "-a", "workspace_attr", "-v", "test_value"])

        with Capturing() as fo:
            ret = call_fiss(["fissfc", "attr_get", "-p", self.namespace, "-w", self.static_workspace,
                             "-a", "workspace_attr"])
        logging.debug(''.join(fo))
        self.assertEquals(''.join(fo), "workspace_attr\ttest_value")
        self.assertEquals(0, ret)

    def test_attr_ops(self):
        """ Test attr_ops on entities"""
        # Upload the 4 test data files
        call_fiss(["fissfc", "entity_import", "-p", self.namespace, "-w", self.static_workspace,
                   "-f", os.path.join("firecloud", "tests", "participants.tsv")])
        call_fiss(["fissfc", "entity_import", "-p", self.namespace, "-w", self.static_workspace,
                   "-f", os.path.join("firecloud", "tests", "samples.tsv")])
        call_fiss(["fissfc", "entity_import", "-p", self.namespace, "-w", self.static_workspace,
                   "-f", os.path.join("firecloud", "tests", "sset_membership.tsv")])
        call_fiss(["fissfc", "entity_import", "-p", self.namespace, "-w", self.static_workspace,
                   "-f", os.path.join("firecloud", "tests", "sset.tsv")])

        # Now call attr_get
        with Capturing() as fo:
            ret = call_fiss(["fissfc", "attr_get", "-p", self.namespace, "-w", self.static_workspace,
                            "-t", "sample_set", "-a", "set_attr_1"])
        logging.debug('\n'.join(fo))

        self.assertEquals('\n'.join(fo), "sample_set_id\tset_attr_1\nSS-NT\tValue-C\nSS-TP\tValue-A")
        self.assertEquals(0, ret)

        # Now call attr_set on a sample_set, followed by attr_get
        call_fiss(["fissfc", "-y", "attr_set", "-p", self.namespace, "-w", self.static_workspace,
                   "-t", "sample_set", "-e", "SS-TP", "-a", "set_attr_1", "-v", "Value-E"])

        with Capturing() as fo2:
            ret = call_fiss(["fissfc", "attr_get", "-p", self.namespace, "-w", self.static_workspace,
                             "-t", "sample_set", "-a", "set_attr_1"])
        logging.debug('\n'.join(fo2))
        self.assertEquals('\n'.join(fo2), "sample_set_id\tset_attr_1\nSS-NT\tValue-C\nSS-TP\tValue-E")
        self.assertEquals(0, ret)

def main():
    nose.main()

if __name__ == '__main__':
    main()
