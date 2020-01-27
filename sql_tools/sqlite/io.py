"""
Contains methods related to connection(s) between database and file.
"""

import time

from numpy import array
from pandas import read_csv

import sql_tools.internals as tools
from sql_tools import constants

from . import sqliteException
from .execute import execute
from .fetch import _pdatabase, _ptableName, getTNames


def _ppath(db):
    if not db:
        raise sqliteException.PathError("Please provide a valid csv path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(db)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise sqliteException.PathError(
                'Invalid path input. Path should be a "str" or "list" type object.'
            )
        db = __temp_lst__.copy()
        del __temp_lst__
    return db


def tbToCsv(tb, db="", returnDict=False, index=False):
    """
    Converts table records to a CSV file.
    """
    constants.__startTime__ = time.time()
    db = _pdatabase(db)
    tb = _ptableName(tb)

    if len(tb) != len(db):
        raise ValueError(
            "Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a matrix."
        )

    final = []
    for i in range(len(db)):
        tools.setStatus(f"Converting database to dataframe ({db[i]})")
        try:
            final.append(
                tools.sqliteTools.__tbToCsv(
                    data=execute(f"SELECT * FROM {tb[i]}")[0],
                    tb=tb[i],
                    db=db[i],
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


def csvToTbl(csv, tb, db="", returnDict=False):
    db = _pdatabase(db)
    tb = _ptableName(tb)
    csv = _ppath(csv)

    for i in range(len(db)):
        try:
            getTNames(db, returnDict=True)[db[i]].index(tb[i])
        except:
            raise sqliteException.TableError(
                f"Table ({tb[i]}) doesn't exists. Create it first to import the data"
            )

    if len(db) != len(tb) or len(tb) != len(csv):
        raise sqliteException.PathError(
            "Please provide equal no. of csv path, table names, and database paths"
        )

    final = []
    for i in range(len(db)):
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

        execute(query, db=db[i])
        final.append(True)

    if returnDict:
        return dict(zip(db, final))
    else:
        return final


def dbToCSV(db="", returnDict=False):
    """
    Converts the data infoformation to a CSV file.
    """
    constants.__startTime__ = time.time()
    db = _pdatabase(db)

    final = []
    for i in range(len(db)):
        tools.setStatus(f"Creating CSV of {db[i]}")
        final.append(
            tools.sqliteTools.__tbToCsv(
                data=execute("SELECT * FROM sqlite_master")[0],
                tb="",
                db=db[i],
                tbl=False,
            )
        )

    if returnDict:
        tools.setStatus("Packing into dictionary")
        final = dict(zip(db, final))

    tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = (
        f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
    )
    return final


if __name__ == "__main__":
    print("File-Database extention for SQL-Tools library.")
