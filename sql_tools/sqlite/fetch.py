"""
File containing methods to fetch data.
"""
from execute import execute
import time


__history__ = []


def getNoOfRecords(tableName, databPath="", returnDict=False):
    """
    Returns the no. of records in the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    startTime = time.time()
    if not databPath:
        databPath = self.databPath
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
            self.__status(f"Gtiing records for {databPath[i]}")
            if "ERROR IN SQL QUERY --->" not in self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False, _Sqlite3__execMethod=False):
                result.append(len(self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False, _Sqlite3__execMethod=False)[0]))
            else:
                raise ValueError(self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False, _Sqlite3__execMethod=False)[0])
        except Exception:
            result.append(0)

    if returnDict:
        self.__status("Packing into dictionary")
        result = dict(zip(tableName, result))

    self.__status("Returning results")

    stopTime = time.time()
    self.execTime = stopTime-startTime
    return result

def getNoOfColumns(tableName, databPath="", returnDict=False):
    """
    Returns the no. of columns in the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    startTime = time.time()
    if not databPath:
        databPath = self.databPath
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
            queryResult = self.getColumnNames(tableName=tableName[i], databPath=databPath[i])
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
        self.__status("Packing into dictionary")
        result = dict(zip(tableName, result))

    stopTime = time.time()
    self.execTime = stopTime-startTime
    return result

def getColumnNames(tableName="", databPath="", returnDict=False):
    """
    Returns the column names of the provided table.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    startTime = time.time()
    if not databPath:
        databPath = self.databPath
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
        self.__status(f"Getting results for {tableName[i]}")
        try:
            queryResult = self.execute(f"PRAGMA table_info({tableName[i]});", databPath=databPath[i], matrix=False, inlineData=False, _Sqlite3__execMethod=False)
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
        self.__status("Packing into dictionary")
        result = dict(zip(databPath, result))

    self.__status("Returning results")
    stopTime = time.time()
    self.execTime = stopTime-startTime
    return result

def getTableNames(databPath="", returnDict=False):
    """
    Returns the table names in the provided database.
    You can provided multiple database paths..
    """
    startTime = time.time()
    if not databPath:
        databPath = self.databPath
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
        self.__status(f"Getting table names for {databPath[i]}")
        queryResult = self.execute(f"SELECT name FROM sqlite_master WHERE type = 'table';", databPath=databPath[i], matrix=False, inlineData=True, _Sqlite3__execMethod=False)
        if "ERROR IN SQL QUERY --->" not in queryResult:
            database = queryResult[0]
            final = []
            for names in database:
                final.append(names[0])
            result.append(final)
        else:
            raise ValueError(queryResult)

    if returnDict:
        self.__status("Packing into dictionary")
        result = dict(zip(databPath, result))

    self.__status("Returning results")
    stopTime = time.time()
    self.execTime = stopTime-startTime
    return result

def getTableCommand(tableName="", databPath="", returnDict=False):
    """
    Returns the command for creating the provided table in the database accordingly.
    You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
    """
    startTime = time.time()
    if not databPath:
        databPath = self.databPath
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
        self.__status(f"Getting results for {tableName[i]}")
        queryResult = self.execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName[i]}';", databPath=databPath[i], matrix=False, inlineData=True, _Sqlite3__execMethod=False)
        if "ERROR IN SQL QUERY --->" not in queryResult:
            result = queryResult
            if result == [[""]]:
                raise ValueError(f"The table doesn't exists. ({tableName[i]})")
            else:
                try:
                    final.append(result[0][0])
                except Exception as e:
                    raise e
        else:
            raise ValueError(self.execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName[i]}';", databPath=databPath, matrix=False, inlineData=True, _Sqlite3__execMethod=False))

    if returnDict:
        self.__status("Packing into dictionary")
        result = dict(zip(tableName, result))

    self.__status("Returning results")
    stopTime = time.time()
    self.execTime = stopTime-startTime
    return final
    """
    Sorts the columns according to their names in accordance to the specified order.
    """
    if isinstance(tableName, list):
        raise ValueError("Multiple table names aren't supported for this method at the moment.")
    if isinstance(databPath, list):
        raise ValueError("Multiple databases aren't supported for this method at the moment.")

    startTime = time.time()
    order = order.lower()
    if not databPath:
        databPath = self.databPath
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
        command = self.getTableCommand(tableName=tableName[i], databPath=databPath[i])[0][0].replace("\n", " ").replace("\t"," ").replace("`", "").split(" ")
        oldCName = self.getColumnNames(tableName=tableName[i], databPath=databPath[i])[0]  # REMOVE 0 FOR MULTIPLE DATABASES
        oldIndex = [command.index(x) for x in oldCName]

        self.__status(f"Getting schema for {tableName[i]}")
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

        self.__status(f"Creating a shallow copy of database {databPath[i]}")
        command = " ".join(command).replace(tableName[i], f"temp_sql_tools_159753_token_copy_{tableName[i]}")

        done = []
        try:
            self.__status("Preparing databases for operation")
            self.execute(command, databPath=databPath[i], _Sqlite3__execMethod=False)
            self.execute(f"INSERT INTO temp_sql_tools_159753_token_copy_{tableName[i]} SELECT * FROM {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
            for j in range(len(oldCName)):
                if oldCName[j] not in done and newCname[j] not in done:
                    self.execute(f"UPDATE temp_sql_tools_159753_token_copy_{tableName[i]} SET {oldCName[j]} = {newCname[j]} , {newCname[j]} = {oldCName[j]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                    done.append(oldCName[i])
            self.execute(f"DROP TABLE {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
            self.execute(f"ALTER TABLE temp_sql_tools_159753_token_copy_{tableName[i]} RENAME TO {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)

            del oldCName, oldIndex, newCname
            final.append(True)
        except Exception as e:
            try:
                self.execute(f"DROP TABLE temp_sql_tools_159753_token_copy_{tableName[i]}", databPath=databPath[i])
            except:
                pass
            raise e

    self.__status("Returning results")
    stopTime = time.time()
    self.execTime = stopTime-startTime
    return final



if __name__ == "__main__":
    print(execute("SELECT * FROM STUDENT", databPath="hello.db", logConsole=True))
