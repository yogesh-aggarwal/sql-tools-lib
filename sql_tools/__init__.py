"""
An integrative library that contains tools for performing various tasks related to the relations (table records).
"""

def path():
    import sys
    import os
    sys.path.insert(0, os.getcwd())
path()

import os
import shutil
import pathlib
import sqlite3

import numpy as np


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

    @property
    def __path__(self):
        return self.databPath

    def execute(self, command, databPath="", matrix=True, inlineData=False, strToList=False, splitByColumns=False):
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
        
        # print(f"databpath (from execute) ---> {databPath}")
        # print(f"command (from execute) ---> {command}")

        data = []
        final = []
        for i in range(len(databPath)):
            conn = sqlite3.connect(databPath[i])
            c = conn.cursor()
            try:
                c.execute(command[i])
            except Exception as e:
                return f"ERROR IN SQL QUERY ---> {e} (From database {databPath[i]})"
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
        
            if splitByColumns:
                __temp = []

                data = np.array(data)
                data = np.transpose(data)

                for i in range(len(data)):
                    __temp.append(data[i].tolist())
                
                data = __temp.copy()
                del __temp

            if data != []:
                if matrix:
                    final.append( np.array(data))
                else:
                    __temp = []
                    if inlineData:
                        for i in range(len(data)):
                            __temp.append(data[i][0])
                        data = __temp.copy()
                        del __temp
                        
                        if strToList:
                            if len(data) == 1:
                                if data[0][0] == "(" or data[0][0] == "[":
                                    def strTolst(query):
                                        intLst = (1, 2, 3, 4, 5, 6, 7, 8, 0)

                                        tempLst = (
                                            query.replace("[", "", 1)
                                            .replace("]", "", 1)
                                            .replace("(", "", 1)
                                            .replace(")", "", 1)
                                            .split(", ")
                                            .copy()
                                        )

                                        query = []
                                        for element in tempLst:
                                            try:
                                                if float(element) in intLst:
                                                    query.append(float(element))
                                                else:
                                                    query.append(element)
                                            except:
                                                query.append(element)
                                        del intLst, tempLst
                                        return query
                                    data = strTolst(data[0])
                        
                        final.append(data)
                    else:
                        final.append(data)
            else:
                return [[""]]

        return final

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
            if "ERROR IN SQL QUERY --->" not in self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False)[0][0]:
                result.append(len(self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False)[0][0]))
            else:
                raise ValueError(self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False)[0][0])
        return result
    
    def getNoOfColumns(self, tableName="", databPath=""):
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
            if "ERROR IN SQL QUERY --->" not in self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False)[0][0][0]:
                result.append(len(self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False)[0][0][0]))
            else:
                raise ValueError(self.execute(f"SELECT * FROM {tableName[i]};", databPath=databPath[i], matrix=False, inlineData=False)[0][0][0])
        return result

    def getColumnNames(self, tableName="", databPath=""):
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
            if "ERROR IN SQL QUERY --->" not in self.execute(f"PRAGMA table_info({tableName[i]});", databPath=databPath[i], matrix=False, inlineData=False)[0][0]:
                temp_result = self.execute(f"PRAGMA table_info({tableName[i]});", databPath=databPath[i], matrix=False, inlineData=False)[0][0]
                final = []
                for value in temp_result:
                    final.append(value[1])
                
                del temp_result

                result.append(final)
            else:
                raise ValueError(self.execute(f"PRAGMA table_info({tableName[i]});", databPath=databPath, matrix=False, inlineData=False)[0][0])
        
        return result

    def getValuesColumnWise(self, tableName="", databPath=""): #PENDING
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
            columns = self.getColumnNames(tableName=tableName[i], databPath=databPath[i])
            values = self.execute(F"SELECT * FROM {tableName[i]}", databPath=databPath[i], matrix=False, splitByColumns=True)
            for j in range(len(columns)):
                result.append(dict(zip(columns[j], values[j])))

        print(f"extract ---> {result}")

        # print(f"__result_temp__ ---> {result}")

        columnsFinal = []
        values = []
        
        final = []
        for __result_temp__ in result:
            __temp__column__ = []
            for i in __result_temp__:
                __temp__column__.append(i)
                # print(columns)
                values.append(__result_temp__[i])
            columnsFinal.append(__temp__column__)
        
        valuesFinal = []
        print(f"COLUMNS ---> {columns}")
        print(values)
        for __result_temp__ in result:
            __temp__ = []
            for value in values:
                __temp__column__ = []
                for j in range(len(value)):
                    __temp__column__.append(value[j][0])
                __temp__.append(__temp__column__)
                print(__temp__)
            valuesFinal.append(__temp__)
            
            # print(columnsFinal)
            # print(valuesFinal)
            
            for element in range(len(columnsFinal)):
                print(element)
                final.append(dict(zip(columnsFinal[element], valuesFinal[element])))
            __temp__.clear()

        # print(dict(zip(columns, __temp__)))
        # print(f"COLUMNS ---> {columns}\nVALUES ---> {__temp__}")
        print(final)
        # print(dict(zip(columns, __temp__)))
        
        del __temp__, __result_temp__, __temp__column__, columns, value, values


        return 0

    def getTableNames(self, databPath=""):
        if "ERROR IN SQL QUERY --->" not in self.execute(f"SELECT name FROM sqlite_master WHERE type = 'table';", databPath=databPath, matrix=False, inlineData=True):
            result = self.execute(f"SELECT name FROM sqlite_master WHERE type = 'table';", databPath=databPath, matrix=False, inlineData=True)
            return result
        else:
            raise ValueError(self.execute(f"SELECT name FROM sqlite_master WHERE type = 'table';", databPath=databPath, matrix=False, inlineData=True))

    def getTableCommand(self, tableName="", databPath=""):
        if "ERROR IN SQL QUERY --->" not in self.execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName}';", databPath=databPath, matrix=False, inlineData=True):
            result = self.execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName}';", databPath=databPath, matrix=False, inlineData=True)
            if result == [[""]]:
                raise ValueError(f"The table doesn't exists. ({tableName})")
            else:
                return result
        else:
            raise ValueError(self.execute(f"SELECT sql FROM sqlite_master WHERE type = 'table' and name='{tableName}';", databPath=databPath, matrix=False, inlineData=True))

    # ==================================================================================================================================================

    def sortRecords(self, tableName="", databPath="", column="", order=""): #PENDING
        result = None
        if "ERROR IN SQL QUERY --->" not in self.execute(f"SELECT * FROM {tableName} ORDER BY {column} {order};", databPath=databPath, matrix=False, inlineData=False):
            try:
                self.execute(self.getTableCommand(tableName="STUDENT")[0].replace(tableName, "copy_357159_753951_sql_tools_table_copy", 1))
            except Exception as e:
                print(e)
                raise ValueError("The table doesn't exists.")
            self.execute(f"INSERT INTO copy_357159_753951_sql_tools_table_copy SELECT * FROM {tableName} ORDER BY {column} {order};", databPath=databPath, matrix=False, inlineData=False)
            self.execute(f"DROP TABLE {tableName}")
            result = self.execute(f"ALTER TABLE copy_357159_753951_sql_tools_table_copy RENAME TO {tableName}", databPath=databPath, matrix=False, inlineData=False)
            return result
        else:
            raise ValueError(self.execute(f"INSERT INTO {tableName} SELECT * FROM {tableName} ORDER BY {column} {order};", databPath=databPath, matrix=False, inlineData=False))

    def swapColumns(self, tableName="", databPath="", column_1="", column_2=""): #PENDING
        self.execute(f"UPDATE {tableName} SET {column_1} = {column_2} AND {column_1} = {column_2}")

    def getTableMethods(self, tableName="", databPath=""): #PENDING
        command = self.getTableCommand(tableName=tableName)[0]

        command = command.split("\t")
        columns = self.getColumnNames(tableName=tableName)
        print(command)
        __temp__1 = []
        __temp__2 = []
        for i in command:
            if i.replace("`", "") in columns:
                __temp__1.append(i)
                __temp__2.append(command[command.index(i) + 1])
        
        columns = []
        attributes = []

        for i in __temp__1:
            columns.append(i.replace("`", ""))

        for i in __temp__2:
            i = i.replace(", ", "").replace(",", "").replace("\n", "").replace("", "")
            

        del __temp__2
        return columns, attributes

    def sortLeftToRight(self, tableName="", databPath="", byColumnName=True): #PENDING
        if not byColumnName:
            values = self.getValuesColumnWise(tableName=tableName, databPath=databPath)
            sorted_values = dict(sorted(values.items(), key=lambda values: values[0]))

            result = self.getTableCommand(tableName=tableName)

            return result
        else:
            values = self.getValuesColumnWise(tableName=tableName, databPath=databPath)
            sorted_values = dict(sorted(values.items(), key=lambda values: values[1]))

            sorted_columnNames = list(sorted_values.keys())
            columnNames = self.getColumnNames(tableName=tableName)
            command = self.getTableCommand(tableName=tableName)[0]

            print(sorted_columnNames)
            print(columnNames)

            print(command)

            def replace_last(source_string, replace_what, replace_with):
                head, _sep, tail = source_string.rpartition(replace_what)
                return head + replace_with + tail

            # for i in range(len(sorted_columnNames)):
                # print(sorted_columnNames[i])
            print(sorted_columnNames)
            
            self.execute(f"ALTER TABLE {tableName} RENAME TO copy_852456_258456_sql_tools_table_copy_{tableName}")


            
            return command


            return result
            


        # return(result)
        
        
















class Mysql():
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
    # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # datab = Sqlite3(databPath=r"C:\Users\Yogesh Aggarwal\Desktop\test.db")
    datab = Sqlite3(databPath=[r"C:\Users\Yogesh Aggarwal\Desktop\test.db", r"C:\Users\Yogesh Aggarwal\Desktop\hello.db", r"C:\Users\Yogesh Aggarwal\Desktop\copy.db"])
    # datab.createDatabase()

    # datab.execute("DROP TABLE STUDENT;")
    # datab.execute("CREATE TABLE STUDENT(C1 TEXT, B1 TEXT);")
    # datab.execute("INSERT INTO STUDENT VALUES('Yogesh', '13');")
    # datab.execute("INSERT INTO STUDENT VALUES('Gautum', '77');")

    # result = datab.execute(["SELECT * FROM STUDENT", ""], matrix=False, inlineData=False)
    # result = datab.getNoOfRecords(tableName="STUDENT", databPath=r"C:\Users\Yogesh Aggarwal\Desktop\test.db")
    # result = datab.getNoOfColumns(tableName=["STUDENT", "STUDENT"])
    # result = datab.getColumnNames(tableName=["STUDENT", "STUDENT", "STUDENT"])
    result = datab.getValuesColumnWise(tableName=["STUDENT", "STUDENT", "STUDENT"])
    # result = datab.sortTable(tableName="STUDENT", column="C2", order="DESC")
    # result = datab.sortLeftToRight(tableName="STUDENT", byColumnName=True)
    # result = datab.getTableMethods(tableName="STUDENT")
    print(f"RESULT ---> {result}")

