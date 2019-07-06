"""
*Not for user. Don't modify, delete, use it.*
Contains tools for sql-tools library to work-properly
"""

import logging

from . import constants


def setStatus(arg, logConsole=False):
    if logConsole:
        # logging.basicConfig(format=f"SQL_CURRENT: ")
        logging.error(arg)  # Change the logging style
    else:
        constants.__history__.append(arg)
    constants.__status__ = arg


if __name__ == "__main__":
    print("Execute extension for SQL-Tools library.")
    print("Note: Don't use it seperately otherwise MAY CAUSE THE PROGRAM TO STOP.")
    input()
