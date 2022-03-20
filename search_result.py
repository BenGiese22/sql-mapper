from enums.action_type import ActionType

class SearchResult:

    def __init__(self, action_type: ActionType, target_table: str, fields: list, from_table: str) -> None:
        self.action_type = action_type
        self.target_table = target_table
        self.fields = fields
        self.from_table = from_table

    def __eq__(self, other):
        if self.action_type == other.action_type and self.target_table == other.target_table and self.fields == other.fields and self.from_table == other.from_table:
            return True
        return False