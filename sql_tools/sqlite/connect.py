"""
Connect extension for SQL-Tools library.
"""

from . import constants, sqliteException


def connect(databPath):
    if isinstance(databPath, str):
        constants.__databPath__.append(databPath)
    elif isinstance(databPath, list) or isinstance(databPath, tuple):
        constants.__databPath__.extend(databPath)
    if databPath == []:
        raise sqliteException.PathError("Please provide a valid database path.")

def disconnect(databPath):
    if isinstance(databPath, str):
        try:
            constants.__databPath__.remove(databPath)
        except ValueError as e:
            raise sqliteException.DatabaseError(e)
    elif isinstance(databPath, list) or isinstance(databPath, tuple):
        constants.__databPath__ = [datab for datab in constants.__databPath__ if datab not in databPath]
    if databPath == []:
        raise sqliteException.PathError("Please provide a valid database path.")

if __name__ == "__main__":
    print("Connect extension for SQL-Tools library.")
    input()
