"""
Database operation extension for SQL-Tools library.
# [Done]
"""

import os
import pathlib
import shutil
import __tools
import constants
from execute import execute



def createDatabase(databPath):
    """
    Creates the databases at the path provided.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    if not databPath:
        if not databPath:
            databPath = f"{os.getcwd()}\\datab.db"
        else:
            databPath = databPath[0]

    __tools.setStatus("Creating database")
    execute("", databPath=databPath, _Sqlite3__execMethod=False)
    __tools.setStatus("Fetching byte results")
    __tools.setStatus("Database created.")
    # LOG ---> Created database at {datab[0]} because of two path in the main instance.
    return True

def moveDatabase(newPath, oldPath=""):
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

def copyDatabase(newPath, oldPath=""):
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
                createDatabase(databPath=newPath)
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
        databPath = constants.__databPath__
        if databPath == None:
            raise ValueError("Please provide the database path")
        else:
            oldPath = databPath

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

def delDatabase(databPath=""):
    """
    Deletes the database at the provided path.
    Caution:
    ---
    This action is irreversible.
    """
    if not databPath:
        databPath = databPath

    os.remove(os.path._getfullpathname(databPath))

