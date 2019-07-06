"""
*Not for user. Don't modify, delete, use it.*
Contains tools for sql-tools library to work-properly
"""

import logging
import constants

__history__ = []


def setStatus(arg, logConsole=False):
    if logConsole:
        # logging.basicConfig(format=f"SQL_CURRENT: ")
        logging.error(arg)  # Change the logging style
    else:
        __history__.append(arg)
    constants.__status__ = arg
