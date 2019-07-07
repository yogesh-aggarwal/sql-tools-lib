"""
Database operation extension for SQL-Tools library.
"""

import os
import shutil

from . import __tools, constants, sqliteException
from .execute import execute


def createDatabase(databPath="", raiseError=True):
    """
    Creates the databases at the path provided.
    Caution:
    ---
    Provide the name of the database in the path.
    """
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
        if isinstance(databPath, str):
            __temp = []
            __temp.append(databPath)
            databPath = __temp.copy()
            del __temp
        elif isinstance(databPath, list) or isinstance(databPath, tuple):
            __temp = []
            __temp.extend(databPath)
            databPath = __temp.copy()
            del __temp
        if databPath == []:
            raise sqliteException.PathError("Please provide a valid database path.")

    final = []
    for i in range(len(databPath)):
        try:
            __tools.setStatus("Creating database")
            execute("", databPath=databPath[i], __execMethod=False)
            __tools.setStatus("Fetching byte results")
            __tools.setStatus("Database created.")
            # LOG ---> Created database at {datab[0]} because of two path in the main instance.
            final.append(True)
        except Exception as e:
            final.append(False)
            if raiseError:
                raise e

    return final

def moveDatabase(newPath, oldPath="", raiseError=True):
    """
    Moves the database from the old path to new path.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    if not newPath:
        raise sqliteException.PathError("Please provide a valid database path (newPath).")
    else:
        if isinstance(newPath, list) or isinstance(newPath, tuple):
            pass
        elif isinstance(newPath, str):
            __temp = []
            __temp.append(newPath)
            newPath = __temp.copy()
            del __temp
        else:
            raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object (newPath).")
    
    if not oldPath:
        raise sqliteException.PathError("Please provide a valid database path (oldPath).")
    else:
        if isinstance(oldPath, list) or isinstance(oldPath, tuple):
            pass
        elif isinstance(oldPath, str):
            __temp = []
            __temp.append(oldPath)
            oldPath = __temp.copy()
            del __temp
        else:
            raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object (oldPath).")
    
    if len(oldPath) != len(newPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal oldPath(s) and newPath(s). Should form a matrix.")

    final = []
    for i in range(len(oldPath)):
        if not oldPath[i]:
            oldPath[i] = constants.__databPath__[i]
            if oldPath[i] == None:
                raise sqliteException.PathError("Please provide a valid database path (oldPath).")
        try:
            os.rename(oldPath[i], newPath[i])
            final.append(True)
        except Exception as e:
            final.append(False)
            if raiseError:
                raise e
    return final

def copyDatabase(newPath, oldPath="", raiseError=True):
    """
    Creates a copy of database from the old path to the new path.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    # New path condition
    if newPath:
        if isinstance(newPath, list) or isinstance(newPath, tuple):
            pass
        elif isinstance(newPath, str):
            __temp = []
            __temp.append(newPath)
            newPath = __temp.copy()
            del __temp
        else:
            raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object (newPath).")
    else:
        raise sqliteException.PathError("Please provide a valid database path (newPath).")

    # Old path condition
    if oldPath:
        if isinstance(oldPath, list) or isinstance(oldPath, tuple):
            pass
        elif isinstance(oldPath, str):
            __temp = []
            __temp.append(oldPath)
            oldPath = __temp.copy()
            del __temp
        else:
            raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object (oldPath).")
    else:
        databPath = constants.__databPath__
        if databPath == []:
            raise ValueError("Please provide the database path")
        else:
            oldPath = databPath

    if len(oldPath) != len(newPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal oldPath(s) and newPath(s). Should form a matrix.")

    final = []
    for i in range(len(newPath)):
        try:
            shutil.copy(oldPath[i], newPath[i])
            final.append(True)
        except Exception as e:
            if " are the same file" in str(e):
                newPath = f"copy_{constants.__copyCount__}_{newPath}"
                constants.__copyCount__ += 1
                shutil.copy(oldPath, newPath)
                return True
            else:
                final.append(False)
                if raiseError:
                    raise e

    return final

def delDatabase(databPath="", raiseError=True):
    """
    Deletes the database at the provided path.
    Caution:
    ---
    This action is irreversible.
    """
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
        if isinstance(databPath, list) or isinstance(databPath, tuple):
            pass
        elif isinstance(databPath, str):
            __temp = []
            __temp.append(databPath)
            databPath = __temp.copy()
            del __temp
        else:
            raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object (databPath).")
    
    final = []
    for database in databPath:
        try:
            os.remove(database)
            final.append(True)
        except Exception as e:
            final.append(False)
            if raiseError:
                raise e
    
    return final

def isIdentical(compareWith, databPath="", raiseError=True):
    if not databPath:
        databPath = constants.__databPath__
        if isinstance(databPath, str):
            databPath = []
            databPath.append(constants.__databPath__)
        elif isinstance(databPath, list) or isinstance(databPath, tuple):
            databPath = []
            databPath.extend(constants.__databPath__)
        if databPath == []:
            raise sqliteException.PathError("Please provide a valid database path (databPath).")
    else:
        if isinstance(databPath, str):
            __temp = []
            __temp.append(databPath)
            databPath = __temp.copy()
            del __temp
        elif isinstance(databPath, list) or isinstance(databPath, tuple):
            __temp = []
            __temp.extend(databPath)
            databPath = __temp.copy()
            del __temp
        if databPath == []:
            raise sqliteException.PathError("Please provide a valid database path (databPath).")

    if compareWith:
        if isinstance(compareWith, str):
            __temp = []
            __temp.append(compareWith)
            compareWith = __temp.copy()
            del __temp
        elif isinstance(compareWith, list) or isinstance(compareWith, tuple):
            __temp = []
            __temp.extend(compareWith)
            compareWith = __temp.copy()
            del __temp
        if compareWith == []:
            raise sqliteException.PathError("Please provide a valid database path (compareWith).")

    if len(databPath) != len(compareWith):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal databPath(s) and compareWith(s). Should form a matrix.")

    final = []
    for i in range(len(databPath)):
        try:
            data1 = open(databPath[i], "rb")
            data2 = open(compareWith[i], "rb")
            data1 = data1.read()
            data2 = data2.read()
        except Exception as e:
            if raiseError:
                raise e

        try:
            final.append(True) if data1 == data2 else final.append(False)
        except Exception as e:
            final.append(False)
            if raiseError:
                raise e
    del data1
    del data2
    return final

def clearDatabase(databPath="", raiseError=True):
    """
    Clears the database provided.
    """
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
        if isinstance(databPath, str):
            __temp = []
            __temp.append(databPath)
            databPath = __temp.copy()
            del __temp
        elif isinstance(databPath, list) or isinstance(databPath, tuple):
            __temp = []
            __temp.extend(databPath)
            databPath = __temp.copy()
            del __temp
        if databPath == []:
            raise sqliteException.PathError("Please provide a valid database path.")
    
    final = []
    for database in databPath:
        try:
            open(database, "w").write("")
            final.append(True)
        except Exception as e:
            final.append(False)
            if raiseError:
                raise e
    
    return final

if __name__ == "__main__":
    print("Simple functions extention for SQL-Tools library.")
    print("Helps to perform simple operations related to location of database(s).")
    input()
