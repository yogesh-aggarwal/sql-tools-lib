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


if __name__ == "__main__":
    print("Connect extension for SQL-Tools library.")
    input()
