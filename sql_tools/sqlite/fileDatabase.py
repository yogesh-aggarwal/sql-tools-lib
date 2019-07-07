"""
Contains methods related to connection(s) between database and file.
"""

import time

from . import __tools, constants, sqliteException
from .execute import execute


def tableToCSV(tableName, databPath="", returnDict=False):
    """
    Converts table records to a CSV file.
    """
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

    __temp_lst__ = []
    __temp_lst__.append(tableName)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
    tableName = __temp_lst__.copy()
    del __temp_lst__

    if len(tableName) != len(databPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a matrix.")

    final = []
    for i in range(len(databPath)):
        __tools.setStatus(f"Converting database to dataframe ({databPath[i]})")
        try:
            final.append(__tools.__tableToCSV(data=execute(f"SELECT * FROM {tableName[i]}")[0], tableName=tableName[i], databPath=databPath[i]))
        except Exception as e:
            raise e

    if returnDict:
        __tools.setStatus("Convering to dictionary")
        final = dict(zip(tableName, final))

    __tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"

def databaseToCSV(databPath="", returnDict=False):
    """
    Converts the data infoformation to a CSV file.
    """
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

    final = []
    for i in range(len(databPath)):
        __tools.setStatus(f"Creating CSV of {databPath[i]}")
        final.append(__tools.__tableToCSV(data=execute("SELECT * FROM sqlite_master")[0], tableName="", databPath=databPath[i], table=False))


    if returnDict:
        __tools.setStatus("Packing into dictionary")
        final = dict(zip(databPath, final))

    __tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"
    return final


if __name__ == "__main__":
    print("File-Database extention for SQL-Tools library.")
    input()
