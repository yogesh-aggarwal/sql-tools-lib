"""
*Not for user. Don't modify, delete, use it.*
Contains tools for sql-tools library to work-properly
"""

import logging
import os

from . import constants, toolsException


def setStatus(arg, logConsole=False, raiseError=True):
    try:
        if logConsole:
            logging.basicConfig(format="[%(process)d] SQL-Tools: %(message)s")
            logging.warning(arg)  # Change the logging style
            constants.__processId__ = os.getpid()
        else:
            constants.__history__.append(arg)
        constants.__status__ = arg
        return True
    except Exception:
        if raiseError:
            raise toolsException.UnknownError("Unable to set the status.")
        else:
            return False

if __name__ == "__main__":
    print("Tools extension for SQL-Tools library.")
    print("Note: Don't use it seperately otherwise MAY CAUSE THE PROGRAM TO STOP.")
