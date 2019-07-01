"""
An integrative library that contains tools for performing various tasks related to the relations (table records).
"""
import datetime
import json
import os
import pathlib
import shutil
import sqlite3
import time

import numpy as np
import pandas as pd

# DATA
__version__ = "SQL Tools version 0.1.8"
__sqliteVersion__ = f"SQL Tools: Sqlite3 version {sqlite3.sqlite_version}"
__mySQLVersion__ = f"SQL Tools: MySQL version 2.2.9"
__SqliteFunctions__ = ["Create database: Sqlite3.createDatabase()",
                       "Move database: Sqlite3.moveDatabase()",
                       "Copy database: Sqlite3.copyDatabase()",
                       "Delete database: Sqlite3.delDatabase()",
                       "Get no. of records in table(s): Sqlite3.getNoOfRecords()",
                       "Get no. of columns in table(s): Sqlite3.getNoOfColumns()",
                       "Get no. of table(s) in database(s): Sqlite3.getNoOfTables()",
                       "Get columns names of table(s): Sqlite3.getColumnNames()",
                       "Get table names of database(s): Sqlite3.getTableNames()",
                       "Get table(s) creation command: Sqlite3.getTableCommand()"]
__help__ = "Visit the documentation for more help or type \"help(sql_tools)\""


class Sqlite3:
    def __init__(self, databPath=""):
        self.databPath = databPath
        if not databPath:
            self.databPath = f"{os.getcwd()}\\datab.db"
        else:
            __temp_lst__ = []
            __temp_lst__.append(databPath)
            if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
                __temp_lst__ = __temp_lst__[0]
            elif isinstance(__temp_lst__[0], str):
                pass
            else:
                raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
            self.databPath = __temp_lst__.copy()
            del __temp_lst__

        self.execTime = None
        self.__history__ = []
        self.status = None

    @property
    def __path__(self):
        return self.databPath

    @property
    def __time__(self):
        return f"Wall time: {self.execTime}s"

    def __status(self, status):
        self.__history__.append(status)
        self.status = status

    def execute(self, command="", databPath="", matrix=True, inlineData=False, splitByColumns=False, pathJSON=False, __execMethod=True, __commit=True):
        """
        Executes the given command to the specified database(s).
        Attributes
        ====
        command
        ---
        The command to be executed. Can execute multiple commands for multiple databases accordingly. Provide a list containing the command for multiple databases.

        databPath
        ---
        The database path. Can cooperate with command list operation accordingly. Provide a list of database path(s) to execute the commands accordingly.

        matrix
        ---
        Whether to convert it to numpy arrays or not. It is recomended to set it `True` when you have to perform large operation for the same result.

        inlineData
        ---
        Whether to combine the data of the database fetched.

        splitByColumns
        ---
        Whether to return the values column-wise. By default it return the data record-wise.
        """
        if __execMethod:
            startTime = time.time()
            self.__status("Starting execution")

        if not databPath:
            if pathJSON:
                databPath = []
                command = []
                with open(pathJSON, "r") as f:
                    try:
                        data = json.loads(f.read())
                    except Exception:
                        raise ValueError("JSON file error. Could be the syntax problem.")
                keys = data.keys()
                for i in keys:
                    for j in data[i]:
                        command.append(data[i][j][0])
                    databPath.append(i)
            else:
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
        __temp_lst__.append(command)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        command = __temp_lst__.copy()
        if len(command) != len(databPath):
            raise ValueError("Cannot apply command to the provided data set. Please provide equal commands and paths. Should form a square matrix.")
        del __temp_lst__

        data = []
        for i in range(len(databPath)):
            conn = sqlite3.connect(databPath[i])
            c = conn.cursor()
            try:
                c.execute(command[i])
            except Exception as e:
                raise ValueError(f"ERROR IN SQL QUERY ---> {e} (From database {databPath[i]})")
            result = []
            try:
                for data_fetched in c.fetchall():
                    result.append(data_fetched)
            except Exception as e:
                raise ValueError(f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {databPath[i]})")
            conn.commit()
            c.close()
            conn.close()
            data.append(result)

        if __execMethod:
            stopTime = time.time()
            self.execTime = stopTime-startTime

        # FOR INLINE DATA
        inlineData = False
        __temp = []
        if inlineData:
            if __execMethod:
                self.__status("Inlining data")
            for values in data:
                for value in values:
                    __temp.append(value)
            data = __temp.copy()
        del __temp


        # FOR SPLITBYCOLUMNS
        __temp = []
        if splitByColumns:
            if __execMethod:
                self.__status("Spliting by columns")
            __temp = []
            for database in data:
                __temp.append(list(zip(*database)))
            data = __temp.copy()
        del __temp


        # FOR MATRIX
        if matrix:
            if __execMethod:
                self.__status("Converting to matrix")
                self.__status("Returning results")
            return np.array(data)
        else:
            if __execMethod:
                self.__status("Returning results")
            return data

    def createDatabase(self, databPath=""):
        """
        Creates the databases at the path provided.
        Caution:
        ---
        Provide the name of the database in the path.
        """
        if not databPath:
            if not self.databPath:
                databPath = f"{os.getcwd()}\\datab.db"
            else:
                databPath = self.databPath[0]

        self.__status("Creating database")
        self.execute("", databPath=databPath, _Sqlite3__execMethod=False)
        self.__status("Fetching byte results")
        self.__status("Database created.")
        # LOG ---> Created database at {datab[0]} because of two path in the main instance.
        return True

    def moveDatabase(self, newPath, oldPath=""):
        """
        Moves the database from the old path to new path.
        Caution:
        ---
        Provide the name of the database in the path.
        """
        if not oldPath:
            oldPath = os.getcwd()
        if not newPath:
            return "Please provide the new path of the database"

        newPath = os.path._getfullpathname(newPath)
        oldPath = os.path._getfullpathname(oldPath)

        os.rename(oldPath, newPath)

    def copyDatabase(self, newPath, oldPath=""):
        """
        Creates a copy of database from the old path to the new path.
        Caution:
        ---
        Provide the name of the database in the path.
        """
        # New path condition
        if newPath:
            if not os.path.isfile(newPath):
                try:
                    self.createDatabase(databPath=newPath)
                    if not os.path.isfile(newPath):
                        raise FileNotFoundError("The specified file/directory doesn't exists")
                except Exception:
                    raise FileNotFoundError("The specified file/directory doesn't exists")
            newPath = os.path._getfullpathname(newPath)
        else:
            newPath = os.getcwd()

        # Old path condition
        if oldPath:
            if not os.path.isfile(oldPath):
                raise FileNotFoundError("The specified file/directory doesn't exists")
            oldPath = os.path._getfullpathname(oldPath)
        else:
            if not self.databPath:
                raise ValueError("Please provide the database path")
            else:
                oldPath = self.databPath

        try:
            shutil.copy(oldPath, newPath)
            return True
        except Exception as e:
            if " are the same file" in str(e):
                newPath = f"{pathlib.PurePath(newPath).parents[0]}\copy_{os.path.basename(newPath)}"
                shutil.copy(oldPath, newPath)
                return True
            else:
                raise IOError(e)

    def delDatabase(self, databPath=""):
        """
        Deletes the database at the provided path.
        Caution:
        ---
        This action is irreversible.
        """
        if not databPath:
            databPath = self.databPath

        os.remove(os.path._getfullpathname(databPath))

    def getNoOfRecords(self, tableName, databPath="", returnDict=False):
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

    def getNoOfColumns(self, tableName, databPath="", returnDict=False):
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

    def getColumnNames(self, tableName="", databPath="", returnDict=False):
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

    def getTableNames(self, databPath="", returnDict=False):
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

    def getTableCommand(self, tableName="", databPath="", returnDict=False):
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

    def __tableToCSV(self, data, tableName, databPath="", table=True, database=True):
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

        databPath = databPath[0]  # REMOVE THIS FOR MULTIPLE DATABASES AS IT WILL FETCH THE FIRST DATABASE ONLY

        if table and database:
            pd.DataFrame(data).to_csv(f"{os.path.basename(databPath)}.{tableName}.csv", index=False)
        elif table:
            pd.DataFrame(data).to_csv(f"{tableName}.csv", index=False)
        elif database:
            pd.DataFrame(data).to_csv(f'{os.path.basename(databPath).replace(".db", "").replace(".db3", "").replace(".sqlite", "").replace(".sqlite3", "")}.csv', index=False)
        else:
            raise AttributeError("One attribute must be provided.")
        stopTime = time.time()
        self.execTime = stopTime-startTime
        return True

    def tableToCSV(self, tableName, databPath="", returnDict=False):
        """
        Converts table records to a CSV file.
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
            self.__status(f"Converting database to dataframe ({databPath[i]})")
            try:
                final.append(self.tableToCSV(data=self.execute(f"SELECT * FROM {tableName[i]}")[0], tableName=tableName[i], databPath=databPath[i]))
            except Exception as e:
                raise e

        if returnDict:
            self.__status("Convering to dictionary")
            final = dict(zip(tableName, final))

        self.__status("Returning results")
        stopTime = time.time()
        self.execTime = stopTime-startTime
        return final

    def databaseToCSV(self, databPath="", returnDict=False):
        """
        Converts the data infoformation to a CSV file.
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

        final = []
        for i in range(len(databPath)):
            self.__status(f"Creating CSV of {databPath[i]}")
            final.append(self.__tableToCSV(data=self.execute("SELECT * FROM sqlite_master")[0], tableName="", databPath=databPath[i], table=False))


        if returnDict:
            self.__status("Packing into dictionary")
            final = dict(zip(databPath, final))

        self.__status("Returning results")
        stopTime = time.time()
        self.execTime = stopTime-startTime
        return final

    def getDatabaseSize(self, databPath="", returnDict=False):
        """
        Returns the database size in bytes.
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

        final = []
        for i in range(len(databPath)):
            self.__status(f"Getting size of {databPath[i]}")
            final.append(f"{os.stat(databPath[i]).st_size} bytes ({os.stat(databPath[i]).st_size * 10**(-6)} MB)")

        if returnDict:
            self.__status("Packing into dictionary")
            final = dict(zip(databPath, final))

        stopTime = time.time()
        self.execTime = stopTime-startTime
        return final

    def getSampleDatabase(self, databPath, bigData=False, openLog=False):
        """
        Creates a sample database in the provided location.\n
        ##### WARNING:
        `bigData=True` may take some time to execute.
        - Small database will contain 2 tables with small dataset.
        - Big database will contain 3 tables with big dataset.
        """
        startTime = time.time()
        try:
            open(databPath, "a+")
        except:
            raise FileNotFoundError("The specified path doesn't exists")
        
        if bigData:
            with open("bigSample.sql", "r") as f:
                query=f.read()
        else:
            with open("smallSample.sql", "r") as f:
                query=f.read()
        if openLog:
            f = open("sampleData.log", "a+")
        query = query.split("\n")
        for i in range(len(query)):
            if openLog:
                _time = datetime.datetime.now().strftime("%H:%M:%S")
                f.write(f"[{_time}]: {query[i]}\n")
            if i != len(query):
                self.execute(query[i], databPath=databPath, _Sqlite3__execMethod=False, _Sqlite3__commit=False)
            else:
                self.execute(query[i], databPath=databPath, _Sqlite3__execMethod=False, _Sqlite3__commit=True)
        f.close()
        if openLog:
            os.startfile("sampleData.log")
        stopTime = time.time()
        del _time
        self.execTime = stopTime-startTime

    def swapColumns(self, tableName, oldCName, newCname, databPath="", returnDict=False):
        """
        Swaps the columns provided.
        """
        if isinstance(tableName, list):
            raise ValueError("Multiple table names aren't supported for this method at the moment.")
        if isinstance(databPath, list):
            raise ValueError("Multiple databases aren't supported for this method at the moment.")
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
            command = self.getTableCommand(tableName=tableName[i], databPath=databPath[i])[0][0].replace("\n", " ").replace("\t"," ").replace("`", "").split(" ")
            oldIndex = [command.index(x) for x in oldCName]

            self.__status(f"Getting schema for {tableName[i]}")

            for j in range(len(newCname)):
                command[oldIndex[j]] = newCname[j]

            self.__status(f"Creating a shallow copy of database {databPath[i]}")
            command = " ".join(command).replace(tableName[i], f"temp_sql_tools_159753_token_copy_{tableName[i]}")

            done = []
            print(newCname)
            print(oldCName)
            try:
                self.__status("Preparing databases for operation")
                self.execute(command, databPath=databPath[i], _Sqlite3__execMethod=False)
                self.execute(f"INSERT INTO temp_sql_tools_159753_token_copy_{tableName[i]} SELECT * FROM {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                for j in range(len(oldCName)):
                    print(newCname[j])
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
        return final


class MySql():
    """
    The Development on this class is not started yet. Don't use it.
    """
    def __init__(self):
        pass

    def connectDatabase(self):
        pass

    def execute(self, command):
        pass



if __name__ == "__main__":
    print("Welcome to the SQL Tools package.")
    with open("HELP", "r") as f:
        print(f.read())
