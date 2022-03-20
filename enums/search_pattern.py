from enum import Enum

TABLE_REGEX = "TABLE_REGEX"
FIELD_REGEX = "(.+)"

class SearchPattern(Enum):
    # UPDATE = "update\sTABLE_REGEX"
    # INSERT_INTO = "insert\s+into\s+TABLE_REGEX"
    # FROM = "from\sTABLE_REGEX"
    # UPDATE_FROM = "update\s(\S+).+from\sTABLE_REGEX"
    # INSERT_INTO_FROM = "insert\s+into\s+(\S+).+from\sTABLE_REGEX"
    # CREATED_TABLE = "create\stable\s(if\s(not\s)?exists\s)?TABLE_REGEX"
    INSERT_INTO_FROM = "insert\s+into\s+([A-Za-z_.$\{\}0-9]+)\sselect\s(.+)from\s([A-Za-z_.$\{\}0-9]+)"
    CREATE_TABLE_LIKE = "create\stable\s(if\s(not\s)?exists\s)?%s\s\(like %s\)" % (TABLE_REGEX,TABLE_REGEX)