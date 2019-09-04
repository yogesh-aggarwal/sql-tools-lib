"""
Connect extension for SQL-Tools library.
"""

from . import constants, sqliteException
from .advTools import validate


def connect(databPath, validateDatabase=True, raiseError=True):
    """
    Connects the provided database to the `data scope` of sqlite.
    """
    try:
        if isinstance(databPath, str):
            constants.__databPath__.append(databPath)
        elif isinstance(databPath, list) or isinstance(databPath, tuple):
            constants.__databPath__.extend(databPath)
        if databPath == []:
            raise sqliteException.PathError("Please provide a valid database path.")
        validate(databPath, raiseError=True) if validateDatabase else False
        return True
    except Exception as e:
        if raiseError:
            raise sqliteException.DatabaseError(f"Error in database(s) provided. {e}")
        else:
            return False


def disconnect(databPath, raiseError=True):
    """
    Disconnects the provided database from the `data scope` of sqlite.
    """
    try:
        if isinstance(databPath, str):
            constants.__databPath__.remove(databPath)
        elif isinstance(databPath, list) or isinstance(databPath, tuple):
            constants.__databPath__ = [
                datab for datab in constants.__databPath__ if datab not in databPath
            ]
        if databPath == []:
            raise sqliteException.PathError("Please provide a valid database(s) path.")
    except ValueError as e:
        raise sqliteException.DatabaseError(f"Error in database(s) provided. {e}")


def isConnected(databPath):
    """
    Checks whether the database(s) is connected or not.
    """
    if isinstance(databPath, list) or isinstance(databPath, tuple) or databPath == "":
        final = []
        for path in constants.__databPath__:
            if path == databPath:
                final.append(True)
            else:
                final.append(False)
        return final
    elif isinstance(databPath, str):
        if databPath in constants.__databPath__:
            return [True]
        else:
            return [False]
    elif isinstance(databPath, dict):
        raise ValueError("Dictionaries are not allowed")
    else:
        return False


if __name__ == "__main__":
    print("Connect extension for SQL-Tools library.")
