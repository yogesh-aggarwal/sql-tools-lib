"""
Contains methods related to connection(s) between database and file.
"""

import time

from . import __tools, constants, sqliteException
from .execute import execute
from .fetch import _pdatabase, _ptableName


def tableToCSV(tableName, databPath="", returnDict=False, index=False):
    """
    Converts table records to a CSV file.
    """
    constants.__startTime__ = time.time()
    databPath = _pdatabase(databPath)
    tableName = _ptableName(tableName)

    if len(tableName) != len(databPath):
        raise ValueError(
            "Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a matrix."
        )

    final = []
    for i in range(len(databPath)):
        __tools.setStatus(f"Converting database to dataframe ({databPath[i]})")
        try:
            final.append(
                __tools.__tableToCSV(
                    data=execute(f"SELECT * FROM {tableName[i]}")[0],
                    tableName=tableName[i],
                    databPath=databPath[i],
                    index=index,
                )
            )
        except Exception as e:
            raise e

    if returnDict:
        __tools.setStatus("Convering to dictionary")
        final = dict(zip(tableName, final))

    __tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = (
        f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
    )
    return final


def databaseToCSV(databPath="", returnDict=False):
    """
    Converts the data infoformation to a CSV file.
    """
    constants.__startTime__ = time.time()
    databPath = _pdatabase(databPath)

    final = []
    for i in range(len(databPath)):
        __tools.setStatus(f"Creating CSV of {databPath[i]}")
        final.append(
            __tools.__tableToCSV(
                data=execute("SELECT * FROM sqlite_master")[0],
                tableName="",
                databPath=databPath[i],
                table=False,
            )
        )

    if returnDict:
        __tools.setStatus("Packing into dictionary")
        final = dict(zip(databPath, final))

    __tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = (
        f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
    )
    return final


if __name__ == "__main__":
    print("File-Database extention for SQL-Tools library.")
