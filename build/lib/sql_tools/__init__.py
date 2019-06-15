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

# def copyDatabase(self, newPath="", database=""):
#     if newPath:
#         newPath = os.path._getfullpathname(newPath)
#     else:
#         newPath = os.getcwd()

#     if not database:
#         database = os.path.basename(self.databPath)
#     else:
#         database = os.path.basename(database)

#     try:
#         shutil.copy(self.databPath,  f"{newPath}\\copy_{database}")
#     except Exception as e:
#         return f"SQL ERROR ---> {e}"

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

    def createDatabase(self, path):
        self.execute("", databPath=path)

    def moveDatabase(self, newPath, oldPath=""):
        if not oldPath:
            oldPath = os.getcwd()
        if not newPath:
            return "Please provide the new path of the database"

        newPath = os.path._getfullpathname(newPath)
        oldPath = os.path._getfullpathname(oldPath)

        try:
            os.rename(oldPath, newPath)
        except Exception as e:
            return e

    def copyDatabase(self, newPath, oldPath=""):  # PENDING
        if not oldPath:
            oldPath = os.getcwd()
        if not newPath:
            return "Please provide the new path of the database"

        try:
            shutil.copy(oldPath, newPath)
        except Exception as e:
            return e

    def delDatabase(self, databPath=""):
        if not databPath:
            databPath = self.databPath

        try:
            os.remove(os.path._getfullpathname(databPath))
        except Exception as e:
            return e


class Mysql():
    def __init__(self):
        pass

    def connectDatabase(self):
        pass

    def execute(self, command):
        pass
