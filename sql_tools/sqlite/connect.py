"""
Connect extension for SQL-Tools library.
"""

from sql_tools import constants

from . import sqliteException
from .advTools import validate


def connect(db, validateDb=True, err=True):
    """
    Connects the provided database to the `data scope` of sqlite.
    """
    try:
        if isinstance(db, str):
            constants.__dbSqlite__.append(db)
        elif isinstance(db, list) or isinstance(db, tuple):
            constants.__dbSqlite__.extend(db)
        if db == []:
            raise sqliteException.PathError("Please provide a valid database path.")
        validate(db, err=True) if validate else False
        return True
    except Exception as e:
        if err:
            raise sqliteException.DatabaseError(f"Error in database(s) provided. {e}")
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
            elif isinstance(db, list) or isinstance(db, tuple):
                constants.__dbSqlite__ = [
                    datab for datab in constants.__dbSqlite__ if datab not in db
                ]
            if db == []:
                raise sqliteException.PathError("Please provide a valid database(s) path.")
        except ValueError as e:
            raise sqliteException.DatabaseError(f"Error in database(s) provided. {e}")
    else:
        constants.__dbSqlite__ = []


def isConnected(db):
    """
    Checks whether the database(s) is connected or not.
    """
    if isinstance(db, list) or isinstance(db, tuple) or db == "":
        final = []
        for path in constants.__dbSqlite__:
            if path == db:
                final.append(True)
            else:
                final.append(False)
        return final
    elif isinstance(db, str):
        if db in constants.__dbSqlite__:
            return [True]
        else:
            return [False]
    elif isinstance(db, dict):
        raise ValueError("Dictionaries are not allowed")
    else:
        return False


if __name__ == "__main__":
    print("Connect extension for SQL-Tools library.")
