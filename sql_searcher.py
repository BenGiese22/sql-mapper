import re
from pathlib import Path
from typing import Tuple

from enums.action_type import ActionType
from enums.search_pattern import SearchPattern

class SqlSearcher:

    def __init__(self) -> None:
        pass

    def search_directory(self, dir_path: Path, search_pattern: SearchPattern, exclude_hidden_files: bool = True) -> list:
        search_func = None
        if search_pattern is SearchPattern.INSERT_INTO_FROM:
            search_func = self.search_file_for_insert_into_from
        elif search_pattern is SearchPattern.CREATED_TABLE:
            search_func = self.search_file_for_create_table
        search_results = []
        for path in dir_path.iterdir():
            if exclude_hidden_files and str(path.name)[0] == '.':
                continue

            if path.is_dir():
                search_results += self.search_directory(path, search_pattern)
            else:
                search_results += search_func(path)
        return search_results

    def search_file_for_create_table(self, file_path: Path) -> list:
        regex = re.compile(SearchPattern.CREATED_TABLE.value, re.IGNORECASE)
        search_results = []
        with file_path.open() as f:
            for line in f:
                match = regex.search(str(line))
                if match:
                    search_results.append(match.group(3))
        return search_results

    # def search_file_for_insert_into_from(self, file_path: Path) -> list:
    #     insert_into_from_regex = re.compile(SearchPattern.INSERT_INTO_FROM.value, re.IGNORECASE)
    #     insert_into_regex = re.compile(SearchPattern.INSERT_INTO.value, re.IGNORECASE)
    #     from_regex = re.compile(SearchPattern.FROM.value, re.IGNORECASE)
    #     update_regex = re.compile(SearchPattern.UPDATE.value, re.IGNORECASE)

    #     search_results = []
    #     insert_table = ""
    #     with file_path.open() as f:
    #         for line in f:
    #             if insert_table != '': # Look for From
    #                 update_match = update_regex.search(str(line))
    #                 if update_match:
    #                     insert_table = ''

    #                 from_match = from_regex.search(str(line))
    #                 if from_match and 'substring' not in str(line).lower():
    #                     search_results.append(((insert_table), from_match.group(1)))
    #                     insert_table = ''
    #             else:
    #                 insert_into_from_match = insert_into_from_regex.search(str(line))
    #                 insert_into_match = insert_into_regex.search(str(line))

    #                 if insert_into_from_match:
    #                     search_results.append((insert_into_from_match.group(1), insert_into_from_match.group(2)))
    #                 elif insert_into_match:
    #                     insert_table = insert_into_match.group(1)
    #     return search_results

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

    def search_file_for_select_statements(self, file_path: Path) -> list:
        raise NotImplementedError("sql_searcher.search_file_for_select_statements is not yet implemented.")

    def search_file_xxx_handle_cte_statements(self, file_path: Path) -> list:
        # TODO implement this functionality into the other search functions.
        raise NotImplementedError("sql_searcher.search_for_cte_statements is not yet implemented.")


    def process_word_by_word(self, file_path: Path) -> list:
        with file_path.open() as f:
            for line in f:
                whitespace_removed_line = re.sub(' +', ' ', str(line))
                line = whitespace_removed_line.strip().replace('\n', '').lower()
                if 'create' or 'insert' or 'update' or 'select' or 'from' in line:
                    action_type, line = self._identify_action_type(line)

        # PROCESS WORD BY WORD
        # OBJECT FORM?
        # SearchResult
        # sql_statement
        # fields
        # insert? update? target_table
        # from target_table
        # join?
        # join on?

        pass

    def _identify_action_type(self, line: str) -> Tuple[ActionType, str]:
        if 'create table' in line:
            index = line.index('create table') + len('create table')
            line = line[index:].strip()
            return ActionType.CREATE, line
        return None, None