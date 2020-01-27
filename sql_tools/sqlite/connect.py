"""
Connect extension for SQL-Tools library.
"""

from sql_tools import constants, exception
from sql_tools import internals as tools

# from .advTools import validate


def connect(db, validateDb=True, err=True):
    """
    Connects the provided database to the `data scope` of sqlite.
    """
    try:
        if isinstance(db, str):
            constants.__dbSqlite__.append(db)
        elif tools.checkInstance(db, list, tuple):
            constants.__dbSqlite__.extend(db)
        if db == []:
            raise exception.PathError("Please provide a valid database path.")
        # validate(db, err=True) if validate else False
        return True
    except Exception as e:
        if err:
            raise exception.DatabaseError(f"Error in database(s) provided. {e}")
        else:
            return False


def disconnect(db="", err=True):
    """
    Disconnects the provided database from the `data scope` of sqlite.
    """
    if db:
        try:
            if isinstance(db, str):
                constants.__dbSqlite__.remove(db)
            elif tools.checkInstance(db, list, tuple):
                constants.__dbSqlite__ = [
                    datab for datab in constants.__dbSqlite__ if datab not in db
                ]
            if db == []:
                raise exception.PathError("Please provide a valid database(s) path.")
        except ValueError as e:
            raise exception.DatabaseError(f"Error in database(s) provided. {e}")
    else:
        constants.__dbSqlite__ = []


if __name__ == "__main__":
    print("Connect extension for SQL-Tools library.")
