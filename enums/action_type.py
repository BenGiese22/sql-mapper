from enum import Enum

class ActionType(Enum):
    CREATE = "create table"
    CREATE_LIKE = "create table like"
    INSERT_INTO_FROM = "insert into from"