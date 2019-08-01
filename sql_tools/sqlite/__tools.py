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


def setStatus(arg, logConsole=False):
    if logConsole:
        logging.basicConfig(format='[%(process)d] SQL Tools: %(message)s')
        logging.warning(arg)  # Change the logging style
    else:
        constants.__history__.append(arg)
    constants.__status__ = arg

def __tableToCSV(data, tableName, databPath="", table=True, database=True, index=False):
    constants.__startTime__ = time.time()
    if not databPath:
        databPath = constants.__databPath__
        if isinstance(databPath, str):
            databPath = []
            databPath.append(constants.__databPath__)
        elif isinstance(databPath, list) or isinstance(databPath, tuple):
            databPath = []
            databPath.extend(constants.__databPath__)
        if databPath == []:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        databPath = __temp_lst__.copy()
        del __temp_lst__

    databPath = databPath[0]  # REMOVE THIS FOR MULTIPLE DATABASES AS IT WILL FETCH THE FIRST DATABASE ONLY

    columns=fetch.getColumnNames(tableName, databPath=databPath)[0]
    data=[["ok"]]
    columns = ["name"]
    print(data, columns)
    if table and database:
        if index != False:
            pd.DataFrame(data, columns=["columns"], index=index).to_csv(f"{os.path.basename(databPath)}.{tableName}.csv")
        else:
            pd.DataFrame(data, columns=["columns"]).to_csv(f"{os.path.basename(databPath)}.{tableName}.csv", index=False)
    elif table:
        if index != False:
            pd.DataFrame(data, columns=columns, index=index).to_csv(f"{tableName}.csv")
        else:
            pd.DataFrame(data, columns=columns).to_csv(f"{tableName}.csv", index=False)

    # else:
    #     raise AttributeError("One attribute must be provided.")
    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"
    return True


if __name__ == "__main__":
    print("Execute extension for SQL-Tools library.")
    print("Note: Don't use it seperately otherwise MAY CAUSE THE PROGRAM TO STOP.")
    input()
