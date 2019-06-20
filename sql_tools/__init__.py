"""
An integrative library that contains tools for performing various tasks related to the relations (table records).
"""
import os
import shutil
import pathlib
import sqlite3
# from . import tools
import tools
import numpy as np

# DATA
__version__ = "SQL Tools version 0.1.7"
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

    def execute(self, command, databPath="", matrix=True, inlineData=False, splitByColumns=False, __execMethod=True):
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
        import time
        startTime = time.time()

        if _Sqlite3__execMethod:
            self.__status("Starting execution")
        
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

        stopTime = time.time()
        self.execTime = stopTime-startTime

        # FOR INLINE DATA
        inlineData = False
        __temp = []
        if inlineData:
            if _Sqlite3__execMethod:
                self.__status("Inlining data")
            for values in data:
                for value in values:
                    __temp.append(value)
            data = __temp.copy()
        del __temp


        # FOR SPLITBYCOLUMNS
        __temp = []
        if splitByColumns:
            if _Sqlite3__execMethod:
                self.__status("Spliting by columns")
            __temp = []
            for database in data:
                __temp.append(list(zip(*database)))
            data = __temp.copy()
        del __temp


        # FOR MATRIX
        if matrix:
            if _Sqlite3__execMethod:
                self.__status("Converting to matrix")
                self.__status("Returning results")
            return np.array(data)
        else:
            if _Sqlite3__execMethod:
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
                except:
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
            except:
                result.append(0)
        
        if returnDict:
            self.__status("Packing into dictionary")
            result = dict(zip(tableName, result))

        self.__status("Returning results")
        return result

    def getNoOfColumns(self, tableName, databPath="", returnDict=False):
        """
        Returns the no. of columns in the provided table.
        You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
        """
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
            except:
                result.append(0)
        if returnDict:
            self.__status("Packing into dictionary")
            result = dict(zip(tableName, result))

        return result

    def getColumnNames(self, tableName="", databPath="", returnDict=False):
        """
        Returns the column names of the provided table.
        You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
        """
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
        return result

    def getTableNames(self, databPath="", returnDict=False):
        """
        Returns the table names in the provided database.
        You can provided multiple database paths..
        """
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
        return result

    def getTableCommand(self, tableName="", databPath="", returnDict=False):
        """
        Returns the command for creating the provided table in the database accordingly.
        You can provided multiple table names and multiple database paths to get the result in group by providing the arguments in a list.
        """
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
        return final

    def sortColumns(self, tableName, databPath="", order="ASC"):  # DO NOT SORT SECOND DATABASE
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
            oldColumns = self.getColumnNames(tableName=tableName[i], databPath=databPath[i])[0]  # REMOVE 0 FOR MULTIPLE DATABASES
            oldIndex = [command.index(x) for x in oldColumns]

            self.__status("Getting table schema")
            newColumns = oldColumns.copy()
            newColumns.sort()

            if order == "desc":
                newColumns.reverse()
            elif order == "asc":
                pass
            else:
                raise AttributeError(f"Invalid order \"{order}\". Accepted orders are \"ASC\" (for ascending) or \"DESC\" (for descending).")


            for j in range(len(newColumns)):
                command[oldIndex[j]] = newColumns[j]
    
            self.__status(f"Creating a shallow copy of database {databPath[i]}")
            command = " ".join(command).replace(tableName[i], f"temp_sql_tools_159753_token_copy_{tableName[i]}")

            done = []
            try:
                self.__status("Preparing databases for operation")
                self.execute(command, databPath=databPath[i], _Sqlite3__execMethod=False)
                self.execute(f"INSERT INTO temp_sql_tools_159753_token_copy_{tableName[i]} SELECT * FROM {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                for j in range(len(oldColumns)):
                    if oldColumns[j] not in done and newColumns[j] not in done:
                        print(oldColumns[j], newColumns[j])
                        self.execute(f"UPDATE temp_sql_tools_159753_token_copy_{tableName[i]} SET {oldColumns[j]} = {newColumns[j]} , {newColumns[j]} = {oldColumns[j]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                        done.append(oldColumns[i])
                self.execute(f"DROP TABLE {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                self.execute(f"ALTER TABLE temp_sql_tools_159753_token_copy_{tableName[i]} RENAME TO {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                
                del oldColumns, oldIndex, newColumns
                final.append(True)
            except Exception as e:
                try:
                    self.execute(f"DROP TABLE temp_sql_tools_159753_token_copy_{tableName[i]}", databPath=databPath[i])
                except:
                    pass
                raise e

        self.__status("Returning results")
        return final

    def tableToCSV(self, tableName, databPath="", returnDict=False):
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
                final.append(tools.Tools().tableToCSV(data=self.execute(f"SELECT * FROM {tableName[i]}")[0], tableName=tableName[i], databPath=databPath[i]))
            except Exception as e:
                raise e
        
        if returnDict:
            self.__status("Convering to dictionary")
            final = dict(zip(tableName, final))

        self.__status("Returning results")
        return final

    def databaseToCSV(self, databPath, returnDict=False):
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
            final.append(tools.Tools().tableToCSV(data=self.execute("SELECT * FROM sqlite_master")[0], tableName="", databPath=databPath[i], table=False))


        if returnDict:
            self.__status("Packing into dictionary")
            final = dict(zip(databPath, final))

        self.__status("Returning results")
        return final

    def getDatabaseSize(self, databPath, returnDict=False):
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

        return final
    


class MySql():
    def __init__(self): 
        pass

    def connectDatabase(self):
        pass

    def execute(self, command):
        pass



if __name__ == "__main__":
    # print("Welcome to the SQL Tools package.")
    # with open("HELP", "r") as f:
    #     print(f.read())
    datab = Sqlite3(databPath="test.db")
    # RESULT = datab.execute(f"SELECT * FROM pwordHack")
    # RESULT = datab.sortColumns(tableName=["pwordHack", "pwordHack"], databPath=["test.db", "hello.db"], order="ASC")
    # RESULT = datab.databaseToCSV(databPath=["test.db", "hello.db"])
    RESULT = datab.getColumnNames(tableName=["pwordHack", "pwords"], databPath=["test.db", "test.db"])
    print(datab.status)
    print(RESULT)

