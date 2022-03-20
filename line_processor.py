import re
from enums.search_pattern import SearchPattern
from enums.action_type import ActionType
from search_result import SearchResult

class LineProcessor:

    def __init__(self) -> None:
        self.create_table_like_regex = re.compile(SearchPattern.CREATE_TABLE_LIKE.value, re.IGNORECASE)
        self.insert_into_from_regex = re.compile(SearchPattern.INSERT_INTO_FROM.value, re.IGNORECASE)

    def process_line(self, line: str) -> None:
        line = line.lower()
        if 'create table' and 'like' in line:
            create_table_like_match = self.create_table_like_regex.search(line)
            if create_table_like_match:
                return SearchResult(ActionType.CREATE_LIKE, create_table_like_match.group(3), [], create_table_like_match.group(4))
        elif 'insert into' and 'from' in line:
            insert_into_from_match = self.insert_into_from_regex.search(line)
            if insert_into_from_match:
                fields = insert_into_from_match.group(2).split(',')
                fields = [field.strip() for field in fields]
                return SearchResult(ActionType.INSERT_INTO_FROM, insert_into_from_match.group(1), fields, insert_into_from_match.group(3))