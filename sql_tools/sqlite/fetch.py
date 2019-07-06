"""
File containing methods to fetch data.
"""
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
        if databPath == None:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        databPath = __temp_lst__.copy()
        del __temp_lst__

    __temp_lst__ = []
    __temp_lst__.append(tableName)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
    tableName = __temp_lst__.copy()
    del __temp_lst__

    if len(tableName) != len(databPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a square matrix.")

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
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}"
    return result

def getNoOfColumns(tableName, databPath="", returnDict=False):
    """
    Returns the no. of columns in the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    constants.__startTime__ = time.time()
    if not databPath:
        databPath = constants.__databPath__
        if databPath == None:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        databPath = __temp_lst__.copy()
        del __temp_lst__

    __temp_lst__ = []
    __temp_lst__.append(tableName)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
    tableName = __temp_lst__.copy()
    del __temp_lst__

    if len(tableName) != len(databPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a square matrix.")

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
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}"
    return result

def getColumnNames(tableName, databPath="", returnDict=False):
    """
    Returns the column names of the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    constants.__startTime__ = time.time()
    if not databPath:
        databPath = constants.__databPath__
        if databPath == None:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        databPath = __temp_lst__.copy()
        del __temp_lst__

    __temp_lst__ = []
    __temp_lst__.append(tableName)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
    tableName = __temp_lst__.copy()
    del __temp_lst__

    if len(tableName) != len(databPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a square matrix.")

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
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}"
    return result

def getTableNames(databPath, returnDict=False):
    """
    Returns the table names in the provided database.
    You can provided multiple database paths..
    """
    constants.__startTime__ = time.time()
    if not databPath:
        databPath = constants.__databPath__
        if databPath == None:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
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
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}"
    return result

def getTableCommand(tableName, databPath="", returnDict=False):
    """
    Returns the command for creating the provided table in the database accordingly.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    constants.__startTime__ = time.time()
    if not databPath:
        databPath = constants.__databPath__
        if databPath == None:
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
        raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a square matrix.")

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
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}"
    return final

def sortColumns(tableName):
    """
    Sorts the columns according to their names in accordance to the specified order.
    """
    if isinstance(tableName, list):
        raise ValueError("Multiple table names aren't supported for this method at the moment.")
    if isinstance(databPath, list):
        raise ValueError("Multiple databases aren't supported for this method at the moment.")

    order = order.lower()
    constants.__startTime__ = time.time()
    if not databPath:
        databPath = constants.__databPath__
        if databPath == None:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        databPath = __temp_lst__.copy()
        del __temp_lst__

    __temp_lst__ = []
    __temp_lst__.append(tableName)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
    tableName = __temp_lst__.copy()
    del __temp_lst__

    if len(tableName) != len(databPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a square matrix.")

    final = []
    for i in range(len(databPath)):
        command = getTableCommand(tableName=tableName[i], databPath=databPath[i])[0][0].replace("\n", " ").replace("\t"," ").replace("`", "").split(" ")
        oldCName = getColumnNames(tableName=tableName[i], databPath=databPath[i])[0]  # REMOVE 0 FOR MULTIPLE DATABASES
        oldIndex = [command.index(x) for x in oldCName]

        __tools.setStatus(f"Getting schema for {tableName[i]}")
        newCname = oldCName.copy()
        newCname.sort()

        if order == "desc":
            newCname.reverse()
        elif order == "asc":
            pass
        else:
            raise AttributeError(f"Invalid order \"{order}\". Accepted orders are \"ASC\" (for ascending) or \"DESC\" (for descending).")


        for j in range(len(newCname)):
            command[oldIndex[j]] = newCname[j]

        __tools.setStatus(f"Creating a shallow copy of database {databPath[i]}")
        command = " ".join(command).replace(tableName[i], f"temp_sql_tools_159753_token_copy_{tableName[i]}")

        done = []
        try:
            __tools.setStatus("Preparing databases for operation")
            execute(command, databPath=databPath[i], __execMethod=False)
            execute(f"INSERT INTO temp_sql_tools_159753_token_copy_{tableName[i]} SELECT * FROM {tableName[i]}", databPath=databPath[i], __execMethod=False)
            for j in range(len(oldCName)):
                if oldCName[j] not in done and newCname[j] not in done:
                    execute(f"UPDATE temp_sql_tools_159753_token_copy_{tableName[i]} SET {oldCName[j]} = {newCname[j]} , {newCname[j]} = {oldCName[j]}", databPath=databPath[i], __execMethod=False)
                    done.append(oldCName[i])
            execute(f"DROP TABLE {tableName[i]}", databPath=databPath[i], __execMethod=False)
            execute(f"ALTER TABLE temp_sql_tools_159753_token_copy_{tableName[i]} RENAME TO {tableName[i]}", databPath=databPath[i], __execMethod=False)

            del oldCName, oldIndex, newCname
            final.append(True)
        except Exception as e:
            try:
                execute(f"DROP TABLE temp_sql_tools_159753_token_copy_{tableName[i]}", databPath=databPath[i])
            except:
                pass
            raise e

    __tools.setStatus("Returning results")
    constants.__stopTime__ = time.time()
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}"
    return final

def getDatabaseSize(databPath="", returnDict=False):
    """
    Returns the database size in bytes.
    """
    constants.__startTime__ = time.time()
    if not databPath:
        databPath = constants.__databPath__
        if databPath == None:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
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
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}"
    return final

def getSampleDatabase(databPath, bigData=False, openLog=False):
    """
    Creates a sample database in the provided location.\n
    ##### WARNING:
    `bigData=True` may take some time to execute.
    - Small database will contain 2 tables with small dataset.
    - Big database will contain 3 tables with big dataset.
    """
    from pathlib import Path
    startTime = time.time()
    try:
        open(databPath, "a+")
    except:
        raise FileNotFoundError("The specified path doesn't exists")
    
    if bigData:
        with open(f"{str(Path.home())}/AppData/Local/Programs/Python/Python37/lib/site-packages/sql_tools/bigSample.sql", "r") as f:
            query=f.read()
    else:
        with open(f"{str(Path.home())}/AppData/Local/Programs/Python/Python37/lib/site-packages/sql_tools/smallSample.sql", "r") as f:
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
    constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}"

if __name__ == "__main__":
    print("Fetch extension for SQL-Tools library.")
    print("Note: It can be used seperately to save memory rather than to import full library.\n\t* Provide database name if used seperately.")
    input()
