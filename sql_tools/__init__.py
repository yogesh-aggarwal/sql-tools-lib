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
    from pathlib import Path
    print("Welcome to the SQL Tools package.")
    with open(f"{str(Path.home())}/AppData/Local/Programs/Python/Python37/lib/site-packages/sql_tools/HELP", "r") as f:
        print(f.read())
        input()
