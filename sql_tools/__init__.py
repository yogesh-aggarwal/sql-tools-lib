"""
An integrative library that contains tools for performing various tasks related to the relations (table records).
"""
import os
import shutil
import pathlib
import sqlite3

import numpy as np

def __version__():
    return "SQL Tools version 0.1.4"

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

    @property
    def __path__(self):
        return self.databPath

    @property
    def __time__(self):
        return f"Wall time: {self.execTime}s"

    def execute(self, command, databPath="", matrix=True, inlineData=False, splitByColumns=False):
        import time

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
            for values in data:
                for value in values:
                    __temp.append(value)
            data = __temp.copy()
        del __temp


        # FOR SPLITBYCOLUMNS
        __temp = []
        if splitByColumns:
            __temp = []  
            print(data)
            for database in data:
                # print(database)
                __temp.append(list(zip(*database)))
            data = __temp.copy()
        del __temp


        # FOR MATRIX
        if matrix:
            return np.array(data)
        else:
            return data

    def createDatabase(self, path=""):
        if not path:
            if not self.databPath:
                path = f"{os.getcwd()}\\datab.db"
            else:
                path = self.databPath[0]
        self.execute("", databPath=path)
        # LOG ---> Created database at {datab[0]} because of two path in the main instance.
        return True

    def moveDatabase(self, newPath, oldPath=""):
        if not oldPath:
            oldPath = os.getcwd()
        if not newPath:
            return "Please provide the new path of the database"

        newPath = os.path._getfullpathname(newPath)
        oldPath = os.path._getfullpathname(oldPath)

        os.rename(oldPath, newPath)

    def copyDatabase(self, newPath="", oldPath=""):
        # New path condition
        if newPath:
            if not os.path.isfile(newPath):
                try:
                    self.createDatabase(path=newPath)
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
        if not databPath:
            databPath = self.databPath

        os.remove(os.path._getfullpathname(databPath))
    
    def getNoOfRecords(self, tableName="", databPath=""):
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
                if "ERROR IN SQL QUERY --->" not in self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False):
                    result.append(len(self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False)[0]))
                else:
                    raise ValueError(self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False)[0])
            except:
                result.append(0)

        return result
    
    def getNoOfColumns(self, tableName="", databPath="", returnDict=False):
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
            result = dict(zip(databPath, result))

        return result

    def getColumnNames(self, tableName="", databPath="", returnDict=False):
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
                queryResult = self.execute(f"PRAGMA table_info({tableName[i]});", databPath=databPath[i], matrix=False, inlineData=False)
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
            result = dict(zip(databPath, result))

        return result

    def getTableNames(self, databPath="", returnDict=False):
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
            queryResult = self.execute(f"SELECT name FROM sqlite_master WHERE type = 'table';", databPath=databPath[i], matrix=False, inlineData=True)
            if "ERROR IN SQL QUERY --->" not in queryResult:
                database = queryResult[0]
                final = []
                for names in database:
                    final.append(names[0])
                result.append(final)
            else:
                raise ValueError(queryResult)
        
        if returnDict:
            result = dict(zip(databPath, result))

        return result

    def getTableCommand(self, tableName="", databPath="", returnDict=False):
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
            queryResult = self.execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName[i]}';", databPath=databPath[i], matrix=False, inlineData=True)
            print(queryResult)
            if "ERROR IN SQL QUERY --->" not in queryResult:
                result = queryResult
                if result == [[""]]:
                    raise ValueError(f"The table doesn't exists. ({tableName[i]})")
                else:
                    final.append(result[0][0])
            else:
                raise ValueError(self.execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName[i]}';", databPath=databPath, matrix=False, inlineData=True))

        if returnDict:
            result = dict(zip(tableName, result))

        return final


class MySql():
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

