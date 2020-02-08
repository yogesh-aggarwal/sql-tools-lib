# """
# File containing methods to fetch data.
# """
from os import stat

import sql_tools.internals as tools
from sql_tools import constants, exception

from . import execute, sampleData


def getNRecords(tbl, db="", err=True, returnDict=False):
    """
    Returns the no. of records in the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    tools.timer("start")
    dbs = tools.parseDbs(db, base="sqlite")
    tbls = tools.parseTables(tbl, dbs)

    final = []
    for i, db in enumerate(dbs):
        tables = []
        for tbl in tbls[i]:
            tools.setStatus(f"Retrieving records for {tbl}")
            try:
                queryResult = execute(f"SELECT * FROM {tbl};", db).get
                if "ERROR IN SQL QUERY --->" not in queryResult:
                    tables.append(len(queryResult[0][0].T[0].tolist()))
            except Exception as e:
                if err:
                    raise e
        final.append(tables)

    if returnDict:
        tools.setStatus("Packing into dictionary")
        final = dict(zip(dbs, final))

    tools.setStatus("Returning results")
    tools.timer("stop")
    return final


def getNColumns(tbl, db="", err=True, returnDict=False):
    """
    Returns the no. of columns in the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    return getCNames(tbl, db, err, returnDict, True)


def getCNames(tbl, db="", err=True, returnDict=False, __len=False):
    """
    Returns the column names of the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    tools.timer("start")
    dbs = tools.parseDbs(db, base="sqlite")
    tbls = tools.parseTables(tbl, dbs)

    final = []
    for i, db in enumerate(dbs):
        tables = []
        for tbl in tbls[i]:
            tools.setStatus(f"Retrieving records for {tbl}")
            try:
                queryResult = execute(f"PRAGMA table_info({tbl});", db).get
                if "ERROR IN SQL QUERY --->" not in queryResult:
                    try:
                        data = queryResult[0][0].T[1].tolist()
                    except IndexError:
                        data = []
                    tables.append(data) if not __len else tables.append(len(data))
            except Exception as e:
                if err:
                    raise e
        final.append(tables)

    if returnDict:
        tools.setStatus("Packing into dictionary")
        final = dict(zip(dbs, final))

    tools.setStatus("Returning results")
    tools.timer("stop")
    return final


def getTNames(db="", err=True, returnDict=False):
    """
    Returns the table names in the provided database.
    You can provided multiple database paths..
    """
    dbs = tools.parseDbs(db, base="sqlite")
    tools.timer("start")
    final = []
    for i, db in enumerate(dbs):
        tables = []
        tools.setStatus(f"Getting table names for {db}")
        queryResult = execute(f"SELECT name FROM sqlite_master WHERE type = 'table';", db).get
        if "ERROR IN SQL QUERY --->" not in queryResult:
            try:
                tables.append(queryResult[0].T[0].T[0].tolist())
            except IndexError:
                tables.append([[]])
        else:
            if err:
                raise ValueError(queryResult)
        final.append(tables[0])

    if returnDict:
        tools.setStatus("Packing into dictionary")
        final = dict(zip(dbs, final))

    tools.setStatus("Returning results")
    tools.timer("stop")
    return final


def getTCommand(tbl, db="", err=True):
    """
    Returns the command for creating the provided table in the database accordingly.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    tools.timer("start")
    dbs = tools.parseDbs(db, base="sqlite")
    tbls = tools.parseTables(tbl, dbs)

    final = []
    for i, db in enumerate(dbs):
        tables = []
        for tbl in tbls[i]:
            tools.setStatus(f"Retrieving records for {tbl}")
            queryResult = execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tbl.strip()}';", db,).list
            if queryResult == [[[]]]:
                queryResult = execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tbl.lower().strip()}';", db,).list
            if queryResult == [[[]]]:
                queryResult = execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tbl.upper().strip()}';", db,).list
            if queryResult == [[[]]]:
                raise exception.TableError("Please provide a valid table name.")
            if "ERROR IN SQL QUERY --->" not in queryResult:
                tables.append(queryResult[0][0][0][0])
            else:
                if err:
                    raise exception.TableError("Invalid table name, no such table exists")
        final.append(tables)
    tools.setStatus("Returning results")
    tools.timer("stop")
    return final


def getDbSize(db="", returnDict=False):
    """
    Returns the database size in bytes.
    """
    tools.timer("start")
    db = tools.parseDbs(db, base="sqlite")

    final = []
    for i in range(len(db)):
        tools.setStatus(f"Getting size of {db[i]}")
        final.append(
            f"{stat(db[i]).st_size} bytes ({stat(db[i]).st_size * 10**(-6)} MB)"
        )

    if returnDict:
        tools.setStatus("Packing into dictionary")
        final = dict(zip(db, final))

    tools.timer("stop")
    return final


def getSampleDb(db, bigData=False):
    """
    Creates a sample database in the provided location.\n
    WARNING:
    `bigData=True` may take some time to execute.
    - Small database will contain 2 tables with small dataset (~5 Minutes).
    - Big database will contain 3 tables with big dataset (~10 Minutes).
    """
    tools.timer("start")
    try:
        open(db, "a+")
    except:
        raise FileNotFoundError("The specified path doesn't exists")

    if bigData:
        query = sampleData._bigSQL
    else:
        query = sampleData._smallSQL
    query = query.split("\n")
    for i in range(len(query)):
        execute(query[i], db=db)

    tools.timer("stop")
    tools.timer("result")


def execTime():
    """
    Returns the execution time.
    """
    return tools.timer("result")


def status():
    """
    Returns the current execution status.
    """
    return constants.__status__


def jsonFormat():
    """
    Returns the format of JSON file for the execute function.
    """
    return constants.__jsonFormat__


def log():
    """
    Returns the log of the execution process.
    """
    return constants.__history__


def pid():
    """
    Returns the log of the process id of recent continous execution.
    """
    return constants.__pid__
