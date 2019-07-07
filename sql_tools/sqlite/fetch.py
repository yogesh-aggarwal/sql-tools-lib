"""
File containing methods to fetch data.
"""
import datetime
import os
import time

from . import __tools, constants, sqliteException
from .execute import execute


def getNoOfRecords(tableName, databPath="", returnDict=False):
    """
    Returns the no. of records in the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
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

    result = []
    for i in range(len(tableName)):
        try:
            __tools.setStatus(f"Gtiing records for {databPath[i]}")
            if "ERROR IN SQL QUERY --->" not in execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False, __execMethod=False):
                result.append(len(execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False, __execMethod=False)[0]))
            else:
                raise ValueError(execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False, __execMethod=False)[0])
        except Exception:
            result.append(0)

    if returnDict:
        __tools.setStatus("Packing into dictionary")
        result = dict(zip(tableName, result))

    __tools.setStatus("Returning results")

    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"
    return result

def getNoOfColumns(tableName, databPath="", returnDict=False):
    """
    Returns the no. of columns in the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
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

    result = []
    for i in range(len(tableName)):
        try:
            queryResult = getColumnNames(tableName=tableName[i], databPath=databPath[i])
        except Exception as e:
            raise e

        try:
            if "ERROR IN SQL QUERY --->" not in queryResult:
                result.append(len(queryResult[0]))
            else:
                raise ValueError(queryResult)
        except Exception:
            result.append(0)
    if returnDict:
        __tools.setStatus("Packing into dictionary")
        result = dict(zip(tableName, result))

    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"
    return result

def getColumnNames(tableName, databPath="", returnDict=False):
    """
    Returns the column names of the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
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

    result = []
    for i in range(len(tableName)):
        __tools.setStatus(f"Getting results for {tableName[i]}")
        try:
            queryResult = execute(f"PRAGMA table_info({tableName[i]});", databPath=databPath[i], matrix=False, inlineData=False, __execMethod=False)
        except Exception as e:
            raise e

        if "ERROR IN SQL QUERY --->" not in queryResult:
            __info__ = queryResult[0]
            final = []
            for table in __info__:
                final.append(table[1])

            result.append(final)
        else:
            raise ValueError(queryResult)

    if returnDict:
        __tools.setStatus("Packing into dictionary")
        result = dict(zip(databPath, result))

    __tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"
    return result

def getTableNames(databPath="", returnDict=False):
    """
    Returns the table names in the provided database.
    You can provided multiple database paths..
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

    result = []
    for i in range(len(databPath)):
        __tools.setStatus(f"Getting table names for {databPath[i]}")
        queryResult = execute(f"SELECT name FROM sqlite_master WHERE type = 'table';", databPath=databPath[i], matrix=False, inlineData=True, __execMethod=False)
        if "ERROR IN SQL QUERY --->" not in queryResult:
            database = queryResult[0]
            final = []
            for names in database:
                final.append(names[0])
            result.append(final)
        else:
            raise ValueError(queryResult)

    if returnDict:
        __tools.setStatus("Packing into dictionary")
        result = dict(zip(databPath, result))

    __tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"
    return result

def getTableCommand(tableName, databPath="", returnDict=False):
    """
    Returns the command for creating the provided table in the database accordingly.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
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
        raise sqliteException.TableError("Invalid table input. Table should be a \"str\" or \"list\" type object.")
    tableName = __temp_lst__.copy()
    del __temp_lst__

    if len(tableName) != len(databPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a matrix.")

    final = []
    for i in range(len(databPath)):
        __tools.setStatus(f"Getting results for {tableName[i]}")
        queryResult = execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName[i]}';", databPath=databPath[i], matrix=False, inlineData=True, __execMethod=False)
        if queryResult == [[]]:
            queryResult = execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName[i].lower().strip()}';", databPath=databPath[i], matrix=False, inlineData=True, __execMethod=False)
        if queryResult == [[]]:
            queryResult = execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName[i].upper().strip()}';", databPath=databPath[i], matrix=False, inlineData=True, __execMethod=False)
        if queryResult == [[]]:
            raise sqliteException.TableError("Please provide a valid table name.")
        if "ERROR IN SQL QUERY --->" not in queryResult:
            result = queryResult
            if result == [[""]]:
                raise ValueError(f"The table doesn't exists. ({tableName[i]})")
            else:
                try:
                    final.append(result[0])
                except Exception as e:
                    raise e
        else:
            raise ValueError(execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName[i]}';", databPath=databPath, matrix=False, inlineData=True, __execMethod=False))

    if returnDict:
        __tools.setStatus("Packing into dictionary")
        result = dict(zip(tableName, result))

    __tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"
    return final

def getDatabaseSize(databPath="", returnDict=False):
    """
    Returns the database size in bytes.
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
        __tools.setStatus(f"Getting size of {databPath[i]}")
        final.append(f"{os.stat(databPath[i]).st_size} bytes ({os.stat(databPath[i]).st_size * 10**(-6)} MB)")

    if returnDict:
        __tools.setStatus("Packing into dictionary")
        final = dict(zip(databPath, final))

    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"
    return final

def getSampleDatabase(databPath, bigData=False, openLog=False):
    """
    # NOT FUNCTIONING PROPERLY. DO NOT USE IT.
    Creates a sample database in the provided location.\n
    ##### WARNING:
    `bigData=True` may take some time to execute.
    - Small database will contain 2 tables with small dataset (~5 Minutes).
    - Big database will contain 3 tables with big dataset (~10 Minutes).
    """
    from pathlib import Path
    constants.__startTime__ = time.time()
    try:
        open(databPath, "a+")
    except:
        raise FileNotFoundError("The specified path doesn't exists")
    
    if bigData:
        with open(f"{str(Path.home())}/AppData/Local/Programs/Python/Python37/lib/site-packages/sql_tools/data/sample_database/sqlite/bigSample.sql", "r") as f:
            query=f.read()
    else:
        with open(f"{str(Path.home())}/AppData/Local/Programs/Python/Python37/lib/site-packages/sql_tools/data/sample_database/sqlite/smallSample.sql", "r") as f:
            query=f.read()
    if openLog:
        f = open("sampleData.log", "a+")
    query = query.split("\n")
    for i in range(len(query)):
        if openLog:
            _time = datetime.datetime.now().strftime("%H:%M:%S")
            f.write(f"[{_time}]: {query[i]}\n")
        if i != len(query):
            execute(query[i], databPath=databPath, _Sqlite3__execMethod=False, _Sqlite3__commit=False)
        else:
            execute(query[i], databPath=databPath, _Sqlite3__execMethod=False, _Sqlite3__commit=True)
    f.close()
    if openLog:
        os.startfile("sampleData.log")
    del _time
    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"


if __name__ == "__main__":
    print("Fetch extension for SQL-Tools library.")
    print("Note: It can be used seperately to save memory rather than to import full library.\n\t* Provide database name if used seperately.")
    input()
