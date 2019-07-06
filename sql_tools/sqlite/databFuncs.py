"""
Database operation extension for SQL-Tools library.
# [Done]
"""

import os
import pathlib
import shutil

from . import __tools, constants, sqliteException
from .execute import execute


def createDatabase(databPath=""):
    """
    Creates the databases at the path provided.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    if not databPath:
        databPath = constants.__databPath__
        if databPath == None:
            raise sqliteException.PathError("Please provide a valid database path.")

    __tools.setStatus("Creating database")
    execute("", databPath=databPath, __execMethod=False)
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
    if not newPath:
        raise sqliteException.PathError("Please provide a valid database path (newPath).")
    
    if not oldPath:
        oldPath = constants.__databPath__
        if oldPath == None:
            raise sqliteException.PathError("Please provide a valid database path (oldPath).")

    newPath = os.path._getfullpathname(newPath)
    oldPath = os.path._getfullpathname(oldPath)

    os.rename(oldPath, newPath)
    return True

def copyDatabase(newPath, oldPath=""):  # Pending
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
        databPath = constants.__databPath__
        if databPath == None:
            raise sqliteException.PathError("Please provide a valid database path.")

    os.remove(os.path._getfullpathname(databPath))
    return True


if __name__ == "__main__":
    print("Simple functions related to location of database(s).")
    input()
