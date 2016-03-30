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
        """Set up test conditions."""
        self.test_workspace = "API_TEST_TEMP"
        self.test_namespace = "broad-firecloud-testing"

        self.static_workspace = "API_TEST_DONT_DELETE"


    def test_get_workspaces(self):
        """Test get_workspaces()."""
        r, c = fapi.get_workspaces()
        print_(r,c)
        self.assertEqual(r.status, 200)

    def test_create_workspace(self):
        """Test create_workspace()."""
        r, c = fapi.create_workspace(self.test_namespace, self.test_workspace)
        print_(r,c)
        self.assertEqual(r.status, 201)

    def test_delete_workspace(self):
        """Test delete_workspace()."""
        print_(fapi.create_workspace(self.test_namespace, self.test_workspace))
        # Ignore errors, if workspace already exists this does nothing 
        r, c = fapi.delete_workspace(self.test_namespace, self.test_workspace)
        print_(r,c)
        self.assertEqual(r.status, 202)

    def test_get_workspace(self):
        """Test get_workspace()."""
        r, c = fapi.get_workspace(self.test_namespace, self.static_workspace)
        print_(r,c)
        self.assertEqual(r.status, 200)

    def test_get_workspace_acl(self):
        """Test get_workspace_acl()."""
        r, c = fapi.get_workspace_acl(self.test_namespace, self.static_workspace)
        print_(r,c)
        self.assertEqual(r.status, 200)

    def test_update_workspace_acl(self):
        """Test update_workspace_acl()."""
        updates = [{ "email": "abaumann.firecloud@gmail.com", 
                     "accessLevel": "READER"}]
        r, c = fapi.update_workspace_acl(self.test_namespace, self.static_workspace,
                                         updates)
        print_(r, c)
        self.assertEqual(r.status, 200)

    def test_lock_unlock_workspace(self):
        """Test lock_workspace(), unlock_workspace()."""
        r, c = fapi.lock_workspace(self.test_namespace, self.static_workspace)
        print_(r, c)
        self.assertEqual(r.status, 204)

        ##Unlock the workspace to clean up
        r, c = fapi.unlock_workspace(self.test_namespace, self.static_workspace)
        print_(r, c)
        self.assertEqual(r.status, 204)

    def test_update_workspace_attributes(self):
        """Test update_workspace_attributes()."""
        updates = [fapi._attr_up("key1", "value1")]
        r, c = fapi.update_workspace_attributes(self.test_namespace, 
                                                self.static_workspace, updates)
        print_(r, c)
        self.assertEqual(r.status, 200)

    def test_clone_workspace(self):
        """Test clone_workspace()."""
        r, c = fapi.clone_workspace(self.test_namespace, self.static_workspace,
                                    self.test_namespace, 
                                    self.static_workspace + "_CLONE")
        print_(r, c)
        self.assertEqual(r.status, 201)

        ##Cleanup, Delete workspace
        r, c = fapi.delete_workspace(self.test_namespace, 
                                     self.static_workspace + "_CLONE")        
        print_(r, c)
        self.assertEqual(r.status, 202)

    def test_ping(self):
        """Test ping()."""
        r, c = fapi.ping()
        print_(r, c)
        self.assertEqual(r.status, 200)

    def test_get_status(self):
        """Test get_status()."""
        r, c = fapi.get_status()
        print_(r, c)
        self.assertEqual(r.status, 200)

    def test_get_billing_projects(self):
        """Test get_billing_projects()."""
        r, c = fapi.get_billing_projects()
        print_(r, c)
        self.assertEqual(r.status, 200)

    ### Unimplemented Tests
    @unittest.skip("Not Implemented")
    def test_abort_sumbission(self):
        """Test abort_sumbission()."""
        pass

    @unittest.skip("Not Implemented")
    def test_copy_config_from_repo(self):
        """Test copy_config_from_repo()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_copy_config_to_repo(self):
        """Test copy_config_to_repo()."""
        pass

    @unittest.skip("Not Implemented")
    def test_copy_entities(self):
        """Test copy_entities()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_create_config(self):
        """Test create_config()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_create_submission(self):
        """Test create_submission()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_config(self):
        """Test delete_config()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_entity(self):
        """Test delete_entity()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_pair(self):
        """Test delete_pair()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_pair_set(self):
        """Test delete_pair_set()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_participant(self):
        """Test delete_participant()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_participant_set(self):
        """Test delete_participant_set()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_sample(self):
        """Test delete_sample()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_sample_set(self):
        """Test delete_sample_set()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_workflow(self):
        """Test delete_workflow()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_config(self):
        """Test get_config()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_config_template(self):
        """Test get_config_template()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_configs(self):
        """Test get_configs()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_entities(self):
        """Test get_entities()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_entities_tsv(self):
        """Test get_entities_tsv()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_entities_with_type(self):
        """Test get_entities_with_type()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_entity(self):
        """Test get_entity()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_entity_types(self):
        """Test get_entity_types()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_inputs_outputs(self):
        """Test get_inputs_outputs()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_method(self):
        """Test get_method()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_repository_config(self):
        """Test get_repository_config()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_repository_config_acl(self):
        """Test get_repository_config_acl()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_repository_configs(self):
        """Test get_repository_configs()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_repository_method_acl(self):
        """Test get_repository_method_acl()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_repository_methods(self):
        """Test get_repository_methods()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_submission(self):
        """Test get_submission()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_submissions(self):
        """Test get_submissions()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_workflow_outputs(self):
        """Test get_workflow_outputs()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_rename_config(self):
        """Test rename_config()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_update_config(self):
        """Test update_config()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_update_repository_config_acl(self):
        """Test update_repository_config_acl()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_update_repository_method_acl(self):
        """Test update_repository_method_acl()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_update_workflow(self):
        """Test update_workflow()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_upload_entities(self):
        """Test upload_entities()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_upload_entities_tsv(self):
        """Test upload_entities_tsv()."""
        pass

    @unittest.skip("Not Implemented")
    def test_validate_config(self):
        """Test validate_config()."""
        pass

def main():
    nose.main()

if __name__ == '__main__':
    main()