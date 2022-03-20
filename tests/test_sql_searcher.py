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
        search_results = self.sql_searcher.process_word_by_word(file_one_path)
        self.assertEqual(len(search_results), 1)
        self.assertTrue(isinstance(search_results[0], Tuple))

    def test_created_table_file_one(self) -> None:
        file_one_path = TEST_FILES_PATH.joinpath("./file_one.py")
        search_results = self.sql_searcher.search_file_for_create_table(file_one_path)
        self.assertEqual(len(search_results), 1)
        self.assertEqual(search_results[0], 'test_table')

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

    @unittest.SkipTest
    def test_search_directory_insert_into_from(self) -> None:
        search_results = self.sql_searcher.search_directory(TEST_FILES_PATH, SearchPattern.INSERT_INTO_FROM)
        self.assertEqual(len(search_results), 3)

    @unittest.SkipTest
    def test_search_directory_created_table(self) -> None:
        search_results = self.sql_searcher.search_directory(TEST_FILES_PATH, SearchPattern.CREATED_TABLE)
        self.assertEqual(len(search_results), 4)

    def test_insert_into_from_file_three(self) -> None:
        file_three_path = TEST_FILES_PATH.joinpath("./file_three.py")
        search_results = self.sql_searcher.search_file_for_insert_into_from(file_three_path)
        self.assertEqual(len(search_results), 1)
        self.assertEqual(search_results[0], ('adjust_api.stats', 'adjust_api.stats_tmp'))
    
    def test_insert_into_from_file_four(self) -> None:
        file_four_path = TEST_FILES_PATH.joinpath("./file_four.sh")
        search_results = self.sql_searcher.search_file_for_insert_into_from(file_four_path)
        self.assertEqual(search_results[0], ('singular_appsflyer_cpe', 'appsflyer_postbacks'))
        self.assertEqual(search_results[1], ('singular_appsflyer_cpe', 'appsflyer_postbacks_king'))
        self.assertEqual(search_results[2], ('singular_appsflyer_cpi', 'appsflyer_postbacks'))
        self.assertEqual(search_results[3], ('singular_appsflyer_cpi', 'appsflyer_postbacks_king'))
        self.assertEqual(search_results[4], ('singular_adjust_cpe', 'adjust_api.event_stats'))
        self.assertEqual(search_results[5], ('singular_adjust_cpi', 'adjust_api.stats'))
        self.assertEqual(search_results[6], ('singular_clients_cpe', 'singular_source'))
        self.assertEqual(search_results[7], ('singular_clients_cpi', 'singular_source'))
        self.assertEqual(search_results[8], ('singular_kochava_cpe', 'kochavas'))
        self.assertEqual(search_results[9], ('singular_kochava_cpi', 'kochavas'))
        self.assertEqual(search_results[10], ('doubledown_appsflyer', 'appsflyer_postbacks'))
        self.assertEqual(search_results[11], ('tmp.singular_export_${mmp}_${mod_adv}_${mnth}', 'singular_${mmp}_cpe'))
        self.assertEqual(search_results[12], ('tmp.singular_export_${mmp}_${mod_adv}_${mnth}', 'singular_${mmp}_cpi'))
        self.assertEqual(search_results[13], ('tmp.singular_export_${mmp}_${mod_adv}_${mnth}', 'doubledown_appsflyer'))        
        self.assertEqual(len(search_results), 14)

if __name__ == '__main__':
    unittest.main()
