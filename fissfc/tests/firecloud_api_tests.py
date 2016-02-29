#! /usr/bin/env python

import unittest
import nose
import fissfc.firecloud_api as fapi

class TestFirecloudAPI(unittest.TestCase):

    def setUp(self):
        """
        Ensure test conditions are appropriate 
        """
        self.test_workspace = "API_NOSE_TEST"
        self.test_namespace = "broad-firecloud-testing"
        pass

    def test_list_workspaces(self):
        """
        Test api call for listing workspaces
        """
        resp, cont = fapi.list_workspaces()

        self.assertEqual(resp.status, 200)
        self.assertNotEqual(len(cont), 0)

    def test_create_and_delete_workspace(self):
        """
        Test API calls for creating and deleting workspaces
        """
        ##Creating a new workspace is a success
        resp, cont = fapi.create_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(resp.status, 201)

        #Trying to create a workspace that already exists is a 409 error
        resp, cont = fapi.create_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(resp.status, 409)

        #Deleting that workspace is a 202
        resp, cont = fapi.delete_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(resp.status, 202)

        #Trying to delete a non-existent workspace is a 404
        resp, cont = fapi.delete_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(resp.status, 404)

    def test_workspace_gets(self):
        """
        Test get methods (workspace info, acl info)
        """
        #check to make sure getting a non-existent workspace is an error
        r, c = fapi.get_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(r.status, 404)
        r, c = fapi.get_workspace_acl(self.test_namespace, self.test_workspace)
        self.assertEqual(r.status, 404)

        ##Create test workspace
        resp, cont = fapi.create_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(resp.status, 201)

        #now check getting the workspace
        r, c = fapi.get_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(r.status, 200)
        self.assertNotEqual(len(c), 0)
        #Get the workspace's ACL
        r, c = fapi.get_workspace_acl(self.test_namespace, self.test_workspace)
        self.assertEqual(r.status, 200)

        #Delete the workspace
        resp, cont = fapi.delete_workspace(self.test_namespace, self.test_workspace)
        self.assertEqual(resp.status, 202)

def main():
    nose.main()

if __name__ == '__main__':
    main()