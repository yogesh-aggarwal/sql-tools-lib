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
import sqlite3

import numpy as np


class Sqlite3:
    def __init__(self, databPath=""):
        if not databPath:
            self.databPath = f"{os.getcwd()}\\datab.db"
        else:
            self.databPath = os.path._getfullpathname(databPath)

    @property
    def __path__(self):
        return self.databPath

    def execute(self, command, databPath="", matrix=True, inlineData=False, strToList=False):
        if not databPath:
            databPath = self.databPath
        else:
            databPath = os.path._getfullpathname(databPath)

        conn = sqlite3.connect(databPath)
        c = conn.cursor()
        try:
            c.execute(command)
        except Exception as e:
            return f"ERROR IN SQL QUERY ---> {e}"
            # print(f"ERROR IN SQL QUERY ---> {e}")
        data = []
        try:
            for data_fetched in c.fetchall():
                data.append(data_fetched)
        except Exception as e:
            print("SQL: SOME ERROR OCCURED.\n---> {e}")
        conn.commit()
        c.close()
        conn.close()

        if data != []:
            if matrix:
                return np.array(data)
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
                                            if int(element) in intLst:
                                                query.append(int(element))
                                            else:
                                                query.append(element)
                                        except:
                                            query.append(element)
                                    del intLst, tempLst
                                    return query
                                data = strTolst(data[0])
                    return data
                else:
                    return data
        else:
            return [[""]]

    def createDatabase(self, path=""):
        if not path:
            if not self.databPath:
                path = f"{os.getcwd()}\\datab.db"
        self.execute("", databPath=path)
        return True

    def moveDatabase(self, newPath, oldPath=""):
        if not oldPath:
            oldPath = os.getcwd()
        if not newPath:
            return "Please provide the new path of the database"

        newPath = os.path._getfullpathname(newPath)
        oldPath = os.path._getfullpathname(oldPath)

        os.rename(oldPath, newPath)

    # def copyDatabase(self, newPath="", oldPath="", newName="", delOldDatab=False):
    #     if newPath:
    #         newPath = os.path._getfullpathname(newPath)
    #     else:
    #         newPath = os.getcwd()

    #     if oldPath:
    #         oldPath = os.path.basename(oldPath)
    #     else:
    #         oldPath = os.path.basename(self.databPath)

    #     if not newName:
    #         shutil.copy(oldPath, f"{newPath}\\copy_{os.path.basename(oldPath)}")
    #         return True
    #     else:
    #         shutil.copy(oldPath, f"{newPath}\\copy_15935778956426_tokenize_sql_tools_base_tempcopyfile{oldPath}")
    #         try:
    #             os.rename(f"{newPath}\\copy_15935778956426_tokenize_sql_tools_base_tempcopyfile{oldPath}",  f"{newPath}\\{newName}")
    #             return True
    #         except Exception as e:
    #             if not delOldDatab:
    #                 os.remove(f"{newPath}\\copy_15935778956426_tokenize_sql_tools_base_tempcopyfile{oldPath}")
    #             if delOldDatab:
    #                 os.remove(f"{newPath}\\{newName}")
    #                 os.rename(f"{newPath}\\copy_15935778956426_tokenize_sql_tools_base_tempcopyfile{oldPath}",  f"{newPath}\\{newName}")
    #             else:
    #                 print(f"---> Please change your database name, a database with same name already exists in the directory provided. ({newPath}{newName})<---")
    #                 raise e

    def copyDatabase(self, newPath="", oldPath="", newName=""):
        # New path condition
        if newPath:
            newPath = os.path._getfinalpathname(newPath)
        else:
            newPath = os.getcwd()

        # Old path condition
        if oldPath:
            oldPath = os.path._getfinalpathname(oldPath)
        else:
            if not self.databPath:
                raise ValueError("Please provide the database path")
            else:
                oldPath = self.databPath
        
        # New name condition
        if not newName:
            newName = "sql_tools_temp_357159_copy_database.db"
        
        try:
            shutil.copy(oldPath, newPath)
        except Exception as e:
            print(e)
            


    def delDatabase(self, databPath=""):
        if not databPath:
            databPath = self.databPath

        os.remove(os.path._getfullpathname(databPath))



























class Mysql():
    def __init__(self):
        pass

    def connectDatabase(self):
        pass

    def execute(self, command):
        pass
