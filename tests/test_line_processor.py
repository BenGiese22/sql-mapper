import unittest
from enums.action_type import ActionType
from line_processor import LineProcessor
from search_result import SearchResult

class LineProcessorTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.line_processor = LineProcessor()

    def test_create_table_like(self) -> None:
        line = "CREATE TABLE test_table (like source_table);"
        expected_result = SearchResult(ActionType.CREATE_LIKE, 'test_table', [], 'source_table')
        actual_result = self.line_processor.process_line(line)
        self.assertEqual(expected_result, actual_result)

    def test_insert_into_select_from(self) -> None:
        line = "INSERT INTO test_table SELECT * FROM source_table;"
        expected_result = SearchResult(ActionType.INSERT_INTO_FROM, 'test_table', ['*'], 'source_table')
        actual_result = self.line_processor.process_line(line)
        self.assertEqual(expected_result, actual_result)

    def test_insert_into_select_from__fields(self) -> None:
        line = "INSERT INTO test_table SELECT id, name, value FROM source_table;"
        expected_result = SearchResult(ActionType.INSERT_INTO_FROM, 'test_table', ['id', 'name', 'value'], 'source_table')
        actual_result = self.line_processor.process_line(line)
        self.assertEqual(expected_result, actual_result)

if __name__ == '__main__':
    unittest.main()