import unittest
from typing import Tuple
from common import TEST_FILES_PATH
from sql_searcher import SearchPattern, SqlSearcher

class SqlSearcherTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.sql_searcher = SqlSearcher()

    def tearDown(self) -> None:
        pass

    def test_insert_into_from_file_one(self) -> None:
        file_one_path = TEST_FILES_PATH.joinpath("./file_one.py")
        search_results = self.sql_searcher.search_file_for_insert_into_from(file_one_path)
        self.assertEqual(len(search_results), 1)
        self.assertTrue(isinstance(search_results[0], Tuple))

    def test_created_table_file_one(self) -> None:
        file_one_path = TEST_FILES_PATH.joinpath("./file_one.py")
        search_results = self.sql_searcher.search_file_for_create_table(file_one_path)
        self.assertEqual(len(search_results), 1)

    def test_insert_into_from_file_two(self) -> None:
        file_two_path = TEST_FILES_PATH.joinpath("./file_two.py")
        search_results = self.sql_searcher.search_file_for_insert_into_from(file_two_path)
        self.assertEqual(len(search_results), 2)
        self.assertEqual(search_results[0], ('adjust_api.cohort_stats_tmp2', 'adjust_api.cohort_stats_tmp'))
        self.assertEqual(search_results[1], ('adjust_api.cohort_stats', 'adjust_api.cohort_stats_tmp2'))
    
    def test_created_table_file_two(self) -> None:
        file_two_path = TEST_FILES_PATH.joinpath("./file_two.py")
        search_results = self.sql_searcher.search_file_for_create_table(file_two_path)
        self.assertEqual(len(search_results), 3)
        self.assertEqual(search_results[0], 'adjust_api.cohort_stats')
        self.assertEqual(search_results[1], 'adjust_api.cohort_stats_tmp')
        self.assertEqual(search_results[2], 'adjust_api.cohort_stats_tmp2')

    def test_update_from(self) -> None:
        file_two_path = TEST_FILES_PATH.joinpath("./file_two.py")
        search_results = self.sql_searcher.search_file_for_update_from(file_two_path)
        self.assertEqual(len(search_results), 1)
        self.assertEqual(search_results[0], ('adjust_api.cohort_stats', 'adjust_api.cohort_stats_tmp2'))

    def test_search_directory_insert_into_from(self) -> None:
        search_results = self.sql_searcher.search_directory(TEST_FILES_PATH, SearchPattern.INSERT_INTO_FROM)
        self.assertEqual(len(search_results), 3)

    def test_search_directory_created_table(self) -> None:
        search_results = self.sql_searcher.search_directory(TEST_FILES_PATH, SearchPattern.CREATED_TABLE)
        self.assertEqual(len(search_results), 4)

        
if __name__ == '__main__':
    unittest.main()
