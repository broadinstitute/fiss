# -*- coding: utf-8 -*-
"""Unit tests for functions used in mop"""

import unittest
from datetime import datetime

import pprint

from firecloud.fiss import get_duplicate_info, get_files_to_keep


def make_file_metadata(file_path, md5, size, time_created, is_in_data_table):
    file_name = file_path.split("/")[-1]
    return {
        "file_name": file_name,
        "file_path": file_path,
        "size": size,
        "md5": md5,
        "time_created": time_created,
        "time_updated": time_created,
        "unique_key": f"{file_name}.{md5}.{size}",
        "is_in_data_table": is_in_data_table
    }


class TestMop(unittest.TestCase):

    def test_get_files_to_keep(self):
        test_bucket = "gs://bucket/"

        # file A that is duplicated, this one is older, is in data table
        file_path_A1 = f"{test_bucket}folder1/A"
        md5_A = "abc"
        size_A = "10"
        time_created_A1 = datetime(2020, 10, 10)
        file_A1_metadata = make_file_metadata(file_path_A1, md5_A, size_A, time_created_A1, True)

        # file A that is duplicated, newer, but not in data table
        file_path_A2 = f"{test_bucket}folder2/A"
        time_created_A2 = datetime(2020, 10, 15)
        file_A2_metadata = make_file_metadata(file_path_A2, md5_A, size_A, time_created_A2, False)

        # make sure those are duplicates
        self.assertEqual(file_A1_metadata['unique_key'], file_A2_metadata['unique_key'])

        # file B that's not duplicated, is not in data table
        file_path_B = f"{test_bucket}folder1/B"
        md5_B = "xyz"
        size_B = "10"
        time_created_B = datetime(2020, 10, 10)
        file_B_metadata = make_file_metadata(file_path_B, md5_B, size_B, time_created_B, False)

        # file C that is duplicated, this one is older, not in data table
        file_path_C1 = f"{test_bucket}folder1/C"
        md5_C = "tuv"
        size_C = "10"
        time_created_C1 = datetime(2020, 10, 10)
        file_C1_metadata = make_file_metadata(file_path_C1, md5_C, size_C, time_created_C1, False)

        # file C that is duplicated, newer, not in data table
        file_path_C2 = f"{test_bucket}folder2/C"
        time_created_C2 = datetime(2020, 10, 15)
        file_C2_metadata = make_file_metadata(file_path_C2, md5_C, size_C, time_created_C2, False)

        # make sure those are duplicates
        self.assertEqual(file_C1_metadata['unique_key'], file_C2_metadata['unique_key'])

        test_bucket_dict = {
            file_path_A1: file_A1_metadata,
            file_path_A2: file_A2_metadata,
            file_path_B: file_B_metadata,
            file_path_C1: file_C1_metadata,
            file_path_C2: file_C2_metadata
        }

        # GET DUPLICATE FILES
        duplicate_files_dict = get_duplicate_info(test_bucket_dict)

        # there should be 3 unique keys in duplicate_files
        self.assertEqual(len(duplicate_files_dict), 3)

        # check how many duplicates of each file
        self.assertEqual(len(duplicate_files_dict[file_A1_metadata['unique_key']]), 2)
        self.assertEqual(len(duplicate_files_dict[file_B_metadata['unique_key']]), 1)
        self.assertEqual(len(duplicate_files_dict[file_C1_metadata['unique_key']]), 2)

        # GET FILES TO KEEP
        files_to_keep = get_files_to_keep(duplicate_files_dict)

        # there should be 3 unique keys in the files_to_keep dict
        self.assertEqual(len(files_to_keep), 3)

        # files to keep
        self.assertTrue(file_path_A1 in files_to_keep)
        self.assertTrue(file_path_B in files_to_keep)
        self.assertTrue(file_path_C2 in files_to_keep)

        # files to delete
        self.assertFalse(file_path_A2 in files_to_keep)
        self.assertFalse(file_path_C1 in files_to_keep)


if __name__ == '__main__':
    unittest.main()

