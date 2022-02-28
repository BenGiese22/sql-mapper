import re
from re import Pattern, Match
from pathlib import Path
from enum import Enum

class SearchPattern(Enum):
    UPDATE = "update\s([A-Za-z_.0-9]+)"
    INSERT_INTO = "insert\sinto\s([A-Za-z_.0-9]+)"
    FROM = "from\s([A-Za-z_.0-9]+)"
    UPDATE_FROM = "update\s(\S+).+from\s([A-Za-z_]+)"
    INSERT_INTO_FROM = "insert\sinto\s(\S+).+from\s([A-Za-z_]+)"
    CREATED_TABLE = "create\stable\s(if\s(not\s)?exists\s)?([A-Za-z_.0-9]+)"


class FileSearch:

    def __init__(self) -> None:
        pass


    def search_file_for_create_table(self, file_path: Path) -> list:
        regex = re.compile(SearchPattern.CREATED_TABLE.value, re.IGNORECASE)
        search_results = []
        with file_path.open() as f:
            for line in f:
                match = regex.search(str(line))
                if match:
                    search_results.append(match.group(3))
        return search_results

    def search_file_for_insert_into_from(self, file_path: Path) -> list:
        insert_into_from_regex = re.compile(SearchPattern.INSERT_INTO_FROM.value, re.IGNORECASE)
        insert_into_regex = re.compile(SearchPattern.INSERT_INTO.value, re.IGNORECASE)
        from_regex = re.compile(SearchPattern.FROM.value, re.IGNORECASE)

        search_results = []
        insert_table = ""
        with file_path.open() as f:
            for line in f:
                if insert_table != '': # Look for From
                    from_match = from_regex.search(str(line))
                    if from_match:
                        search_results.append(((insert_table), from_match.group(1)))
                        insert_table = ''
                else:
                    insert_into_from_match = insert_into_from_regex.search(str(line))
                    insert_into_match = insert_into_regex.search(str(line))

                    if insert_into_from_match:
                        search_results.append((insert_into_from_match.group(1), insert_into_from_match.group(2)))
                    elif insert_into_match:
                        insert_table = insert_into_match.group(1)
        return search_results

    def search_file_for_select_statements(self, file_path: Path) -> list:
        pass

    def search_file_for_update_from(self, file_path: Path) -> list:
        update_from_regex = re.compile(SearchPattern.UPDATE_FROM.value, re.IGNORECASE)
        update_regex = re.compile(SearchPattern.UPDATE.value, re.IGNORECASE)
        from_regex = re.compile(SearchPattern.FROM.value, re.IGNORECASE)

        search_results = []
        update_table = ""
        with file_path.open() as f:
            for line in f:
                if update_table != '': # Look for From
                    from_match = from_regex.search(str(line))
                    if from_match:
                        search_results.append(((update_table), from_match.group(1)))
                        update_table = ''
                else:
                    update_from_match = update_from_regex.search(str(line))
                    update_match = update_regex.search(str(line))

                    if update_from_match:
                        search_results.append((update_from_match.group(1), update_from_match.group(2)))
                    elif update_match:
                        update_table = update_match.group(1)
        return search_results
