"""
*Not for user. Don't modify, delete, use it.*
Contains tools for sql-tools library to work-properly
"""

import os
import time

import numpy as np
import pandas as pd

from . import constants, fetch


def __tbToCsv(data, tblName, databPath="", tbl=True, database=True, index=False):
    constants.__startTime__ = time.time()
    databPath = fetch._pdatabase(databPath)

    databPath = databPath[
        0
    ]  # REMOVE THIS FOR MULTIPLE DATABASES AS IT WILL FETCH THE FIRST DATABASE ONLY

    columns = fetch.getCNames(tblName, databPath=databPath)[0]
    if tbl and database:
        if index != False:
            pd.DataFrame(data, columns=columns, index=index).to_csv(
                f"{os.path.basename(databPath)}.{tblName}.csv"
            )
        else:
            pd.DataFrame(data, columns=columns).to_csv(
                f"{os.path.basename(databPath)}.{tblName}.csv", index=False
            )
    elif tbl:
        if index != False:
            pd.DataFrame(data, columns=columns, index=index).to_csv(f"{tblName}.csv")
        else:
            pd.DataFrame(data, columns=columns).to_csv(f"{tblName}.csv", index=False)

    # else:
    #     raise AttributeError("One attribute must be provided.")
    constants.__stopTime__ = time.time()
    constants.__time__ = (
        f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
    )
    return True


def dataType(data):
    try:
        dtype = np.array(data).dtype
        print(dtype)
        if (
            dtype == "O"
            or dtype == "<U1"
            or dtype == "<U5"
            or dtype == "<U6"
            or dtype == "<U7"
            or dtype == "<U11"
            or dtype == "<U21"
        ):
            return "str"
        elif dtype == "int64" or dtype == "<U2":
            return "int"
        else:
            return "None"

    except Exception:
        pass


if __name__ == "__main__":
    print("Tools extension for SQL-Tools library.")
    print("Note: Don't use it seperately otherwise MAY CAUSE THE PROGRAM TO STOP.")
