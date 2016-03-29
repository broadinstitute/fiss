#! /usr/bin/env python

import unittest
import nose
import firecloud.api as fapi
from six import print_

class TestFirecloudAPI(unittest.TestCase):
    """Unit test the firecloud.api module.

    There should be at least one test per api call, 
    with composite tests as feasible.
    """

    def setUp(self):
        """
        Ensure test conditions are appropriate 
        """
        self.test_workspace = "FISS_NOSE_TEST"
        self.test_namespace = "broad-firecloud-testing"
        pass

    def test_get_workspaces(self):
        """Test api call for listing workspaces"""
        print_("Test get_workspaces()")
        r, c = fapi.get_workspaces()

        self.assertEqual(r.status, 200)

    def test_create_and_delete_workspace(self):
        """Test API calls for creating and deleting workspaces"""
        ##Creating a new workspace is a success
        print_("Test create_workspace()")
        resp, cont = fapi.create_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(resp.status, 201)

        print_("Test delete_workspace()")
        #Deleting that workspace is a 202
        resp, cont = fapi.delete_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(resp.status, 202)

    def test_workspace_actions(self):
        """Test api calls on workspaces"""
        ##Create test workspace
        resp, cont = fapi.create_workspace(self.test_namespace, self.test_workspace)

        print_("Test get_workspace()")
        #now check getting the workspace
        r, c = fapi.get_workspace(self.test_namespace, self.test_workspace)
        print_(r, c)
        self.assertEqual(r.status, 200)
        self.assertNotEqual(len(c), 0)

        print_("Test get_workspace_acl()")
        #Get the workspace's ACL
        r, c = fapi.get_workspace_acl(self.test_namespace, self.test_workspace)
        print_(r, c)
        self.assertEqual(r.status, 200)


        print_("Test update_workspace_acl()")
        updates = [{ "email": "abaumann.firecloud@gmail.com", 
                     "accessLevel": "WRITER"}]

        print_("Test update_workspace_acl()")
        r, c = fapi.update_workspace_acl(self.test_namespace, self.test_workspace,
                                         updates)
        print_(r, c)
        self.assertEqual(r.status, 200)


        print_("Test lock_workspace()")
        r, c = fapi.lock_workspace(self.test_namespace, self.test_workspace)
        print_(r, c)
        self.assertEqual(r.status, 204)

        print_("Test unlock_workspace()")
        r, c = fapi.unlock_workspace(self.test_namespace, self.test_workspace)
        print_(r, c)
        self.assertEqual(r.status, 204)

        print_("Test update_workspace_attributes()")
        updates = [fapi._attr_up("key1", "value1")]
        r, c = fapi.update_workspace_attributes(self.test_namespace, 
                                                self.test_workspace, updates)
        print_(r, c)
        self.assertEqual(r.status, 200)

        print_("Test clone_workspace()")
        r, c = fapi.clone_workspace(self.test_namespace, self.test_workspace,
                                    self.test_namespace, self.test_workspace + "_CLONE")
        print_(r, c)
        self.assertEqual(r.status, 201)

        #Cleanup, delete the workspaces
        resp, cont = fapi.delete_workspace(self.test_namespace, self.test_workspace)
        print_(r, c)
        self.assertEqual(resp.status, 202)

        resp, cont = fapi.delete_workspace(self.test_namespace, self.test_workspace + "_CLONE")
        print_(r, c)
        self.assertEqual(resp.status, 202)

def main():
    nose.main()

if __name__ == '__main__':
    main()