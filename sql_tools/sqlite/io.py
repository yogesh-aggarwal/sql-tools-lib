"""
Contains methods related to connection(s) between database and file.
"""

import time

from . import tools, constants, sqliteException
from pandas import read_csv
from numpy import array

from .execute import execute
from .fetch import _pdatabase, _ptableName, getTNames


def _ppath(databPath):
    if not databPath:
        raise sqliteException.PathError("Please provide a valid csv path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise sqliteException.PathError(
                'Invalid path input. Path should be a "str" or "list" type object.'
            )
        databPath = __temp_lst__.copy()
        del __temp_lst__
    return databPath


def tbToCsv(tb, databPath="", returnDict=False, index=False):
    """
    Converts table records to a CSV file.
    """
    constants.__startTime__ = time.time()
    databPath = _pdatabase(databPath)
    tb = _ptableName(tb)

    if len(tb) != len(databPath):
        raise ValueError(
            "Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a matrix."
        )

    final = []
    for i in range(len(databPath)):
        tools.setStatus(f"Converting database to dataframe ({databPath[i]})")
        try:
            final.append(
                tools.__tbToCsv(
                    data=execute(f"SELECT * FROM {tb[i]}")[0],
                    tb=tb[i],
                    databPath=databPath[i],
                    index=index,
                )
            )
        except Exception as e:
            raise e

    if returnDict:
        tools.setStatus("Convering to dictionary")
        final = dict(zip(tb, final))

    tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = (
        f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
    )
    return final


def csvToTbl(csv, tb, databPath="", returnDict=False):
    databPath = _pdatabase(databPath)
    tb = _ptableName(tb)
    csv = _ppath(csv)

    for i in range(len(databPath)):
        try:
            getTNames(databPath, returnDict=True)[databPath[i]].index(tb[i])
        except:
            raise sqliteException.TableError(
                f"Table ({tb[i]}) doesn't exists. Create it first to import the data"
            )

    if len(databPath) != len(tb) or len(tb) != len(csv):
        raise sqliteException.PathError(
            "Please provide equal no. of csv path, table names, and database paths"
        )

    final = []
    for i in range(len(databPath)):
        try:
            data = read_csv(csv[i])
        except:
            raise sqliteException.PathError(f"Invalid csv or doesn't exists ({csv[i]})")
        # if data == [[]]:
        #     exit()

        columns = list(data.columns)

        cData = []
        for j in columns:
            cData.append(list(data[j]))

        cData = array(cData).T

        sqlData = []
        for j in cData:
            value = []
            for record in j:
                if tools.dataType(record) == "str":
                    value.append(f"'{record}'")
                else:
                    value.append(record)

            sqlData.append(f'({", ".join(value)})')

        query = f'INSERT INTO {tb[i]} VALUES {", ".join(sqlData)}'
        if query == f"INSERT INTO {tb[i]} VALUES ":
            raise sqliteException.UnknownError(f"Error in csv file ({csv[i]})")

        execute(query, databPath=databPath[i])
        final.append(True)

    if returnDict:
        return dict(zip(databPath, final))
    else:
        return final


def dbToCSV(databPath="", returnDict=False):
    """
    Converts the data infoformation to a CSV file.
    """
    constants.__startTime__ = time.time()
    databPath = _pdatabase(databPath)

    final = []
    for i in range(len(databPath)):
        tools.setStatus(f"Creating CSV of {databPath[i]}")
        final.append(
            tools.__tbToCsv(
                data=execute("SELECT * FROM sqlite_master")[0],
                tb="",
                databPath=databPath[i],
                tbl=False,
            )
        )

    if returnDict:
        tools.setStatus("Packing into dictionary")
        final = dict(zip(databPath, final))

    tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = (
        f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
    )
    return final


if __name__ == "__main__":
    print("File-Database extention for SQL-Tools library.")
