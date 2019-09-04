"""
*Not for user. Don't modify, delete, use it.*
Contains tools for sql-tools library to work-properly
"""

import logging
import os
import time
from . import fetch

import pandas as pd

from . import constants, sqliteException


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
            raise sqliteException.UnknownError("Unable to set the status.")
        else:
            return False


def __tableToCSV(data, tableName, databPath="", table=True, database=True, index=False):
    constants.__startTime__ = time.time()
    databPath = fetch._pdatabase(databPath)

    databPath = databPath[
        0
    ]  # REMOVE THIS FOR MULTIPLE DATABASES AS IT WILL FETCH THE FIRST DATABASE ONLY

    columns = fetch.getColumnNames(tableName, databPath=databPath)[0]
    if table and database:
        if index != False:
            pd.DataFrame(data, columns=columns, index=index).to_csv(
                f"{os.path.basename(databPath)}.{tableName}.csv"
            )
        else:
            pd.DataFrame(data, columns=columns).to_csv(
                f"{os.path.basename(databPath)}.{tableName}.csv", index=False
            )
    elif table:
        if index != False:
            pd.DataFrame(data, columns=columns, index=index).to_csv(f"{tableName}.csv")
        else:
            pd.DataFrame(data, columns=columns).to_csv(f"{tableName}.csv", index=False)

    # else:
    #     raise AttributeError("One attribute must be provided.")
    constants.__stopTime__ = time.time()
    constants.__time__ = (
        f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
    )
    return True


if __name__ == "__main__":
    print("Tools extension for SQL-Tools library.")
    print("Note: Don't use it seperately otherwise MAY CAUSE THE PROGRAM TO STOP.")
