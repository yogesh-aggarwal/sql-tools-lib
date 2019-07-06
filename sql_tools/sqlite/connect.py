"""
Connect extension for SQL-Tools library.
"""

from . import constants


def connect(databPath):
    constants.__databPath__ = databPath


if __name__ == "__main__":
    print("Connect extension for SQL-Tools library.")
    input()
