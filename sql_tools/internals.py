import datetime
import logging
import time
from os import getpid, path

from numpy import array
from pandas import DataFrame

from sql_tools import constants

from . import exception
from .mysql import execute
from .sqlite import fetch


def setStatus(arg, err=True, verbose=False):
    try:
        if verbose:
            logging.basicConfig(format="[%(process)d] SQL-Tools: %(message)s")
            logging.warning(arg)  # Change the logging style
            constants.__pid__ = getpid()
        else:
            constants.__history__.append(arg)
        constants.__status__ = arg
        return True
    except Exception:
        if err:
            raise exception.DatabaseError("Unable to set the status.")
        return False


def checkInstance(value, *args):
    return True if [x for x in args if isinstance(value, x)] else False


def timer(method, execMethod=True, verbose=False):
    if method == "start":
        constants.__startTime__ = datetime.now()
        setStatus("Starting execution", verbose=verbose)
    elif method == "stop":
        constants.__stopTime__ = datetime.now()
        setStatus("Calculating time", verbose=verbose)
    elif method == "result":
        constants.__time__ = f"Wall time: {(constants.__stopTime__ - constants.__startTime__).total_seconds()}s"
        return constants.__time__


# &MySQL
def parseDbs(db):
    db = constants.__dbMysql__ if not db else db
    return execute.execute([], db, _execute__execMethod=False)._execute__parseDatabase()


def parseTables(tables, db):
    return execute.execute(
        tables, db, _execute__execMethod=False
    )._execute__parseCommands()


# &SQLite
def __tbToCsv(data, tblName, db="", tbl=True, database=True, index=False):
    constants.__startTime__ = time.time()
    db = fetch._pdatabase(db)

    db = db[
        0
    ]  # REMOVE THIS FOR MULTIPLE DATABASES AS IT WILL FETCH THE FIRST DATABASE ONLY

    columns = fetch.getCNames(tblName, db=db)[0]
    if tbl and database:
        if index != False:
            DataFrame(data, columns=columns, index=index).to_csv(
                f"{path.basename(db)}.{tblName}.csv"
            )
        else:
            DataFrame(data, columns=columns).to_csv(
                f"{path.basename(db)}.{tblName}.csv", index=False
            )
    elif tbl:
        if index != False:
            DataFrame(data, columns=columns, index=index).to_csv(f"{tblName}.csv")
        else:
            DataFrame(data, columns=columns).to_csv(f"{tblName}.csv", index=False)

    # else:
    #     raise AttributeError("One attribute must be provided.")
    constants.__stopTime__ = time.time()
    constants.__time__ = (
        f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
    )
    return True


def dataType(data):
    try:
        dtype = array(data).dtype
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
