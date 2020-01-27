"""
Database operation extension for SQL-Tools library.
"""

import os
import shutil

import sql_tools.internals as tools
from sql_tools import constants

from . import advTools, sqliteException
from .execute import execute


def createDb(db="", err=True):
    """
    Creates the databases at the path provided.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    if not db:
        db = constants.__dbSqlite__
        if isinstance(db, str):
            db = []
            db.append(constants.__dbSqlite__)
        elif isinstance(db, list) or isinstance(db, tuple):
            db = []
            db.extend(constants.__dbSqlite__)
        if db == []:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        if isinstance(db, str):
            __temp = []
            __temp.append(db)
            db = __temp.copy()
            del __temp
        elif isinstance(db, list) or isinstance(db, tuple):
            __temp = []
            __temp.extend(db)
            db = __temp.copy()
            del __temp
        if db == []:
            raise sqliteException.PathError("Please provide a valid database path.")

    final = []
    for i in range(len(db)):
        try:
            tools.setStatus("Creating database")
            execute("", db=db[i], __execMethod=False)
            tools.setStatus("Fetching byte results")
            tools.setStatus("Database created.")
            # LOG ---> Created database at {datab[0]} because of two path in the main instance.
            final.append(True)
        except Exception as e:
            final.append(False)
            if err:
                raise e

    return final


def moveDb(newPath, oldPath="", err=True):
    """
    Moves the database from the old path to new path.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    if not newPath:
        raise sqliteException.PathError(
            "Please provide a valid database path (newPath)."
        )
    else:
        if isinstance(newPath, list) or isinstance(newPath, tuple):
            pass
        elif isinstance(newPath, str):
            __temp = []
            __temp.append(newPath)
            newPath = __temp.copy()
            del __temp
        else:
            raise sqliteException.PathError(
                'Invalid path input. Path should be a "str" or "list" type object (newPath).'
            )

    if not oldPath:
        raise sqliteException.PathError(
            "Please provide a valid database path (oldPath)."
        )
    else:
        if isinstance(oldPath, list) or isinstance(oldPath, tuple):
            pass
        elif isinstance(oldPath, str):
            __temp = []
            __temp.append(oldPath)
            oldPath = __temp.copy()
            del __temp
        else:
            raise sqliteException.PathError(
                'Invalid path input. Path should be a "str" or "list" type object (oldPath).'
            )

    if len(oldPath) != len(newPath):
        raise ValueError(
            "Cannot apply command to the provided data set. Please provide equal oldPath(s) and newPath(s). Should form a matrix."
        )

    final = []
    for i in range(len(oldPath)):
        if not oldPath[i]:
            oldPath[i] = constants.__dbSqlite__[i]
            if oldPath[i] == None:
                raise sqliteException.PathError(
                    "Please provide a valid database path (oldPath)."
                )
        try:
            os.rename(oldPath[i], newPath[i])
            final.append(True)
        except Exception as e:
            final.append(False)
            if err:
                raise e
    return final


def copyDb(newPath, oldPath="", err=True):
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
            raise sqliteException.PathError(
                'Invalid path input. Path should be a "str" or "list" type object (newPath).'
            )
    else:
        raise sqliteException.PathError(
            "Please provide a valid database path (newPath)."
        )

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
            raise sqliteException.PathError(
                'Invalid path input. Path should be a "str" or "list" type object (oldPath).'
            )
    else:
        db = constants.__dbSqlite__
        if db == []:
            raise ValueError("Please provide the database path")
        else:
            oldPath = db

    if len(oldPath) != len(newPath):
        raise ValueError(
            "Cannot apply command to the provided data set. Please provide equal oldPath(s) and newPath(s). Should form a matrix."
        )

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
                if err:
                    raise e

    return final


def delDb(db="", err=True):
    """
    Deletes the database at the provided path.
    Caution:
    ---
    This action is irreversible.
    """
    if not db:
        db = constants.__dbSqlite__
        if isinstance(db, str):
            db = []
            db.append(constants.__dbSqlite__)
        elif isinstance(db, list) or isinstance(db, tuple):
            db = []
            db.extend(constants.__dbSqlite__)
        if db == []:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        if isinstance(db, list) or isinstance(db, tuple):
            pass
        elif isinstance(db, str):
            __temp = []
            __temp.append(db)
            db = __temp.copy()
            del __temp
        else:
            raise sqliteException.PathError(
                'Invalid path input. Path should be a "str" or "list" type object (db).'
            )

    final = []
    for database in db:
        try:
            os.remove(database)
            final.append(True)
        except Exception as e:
            final.append(False)
            if err:
                raise e

    return final


def isIdentical(compareWith, db="", err=True):
    """
    Returns whether the database(s) are identical or not.
    """
    if not db:
        db = constants.__dbSqlite__
        if isinstance(db, str):
            db = []
            db.append(constants.__dbSqlite__)
        elif isinstance(db, list) or isinstance(db, tuple):
            db = []
            db.extend(constants.__dbSqlite__)
        if db == []:
            raise sqliteException.PathError(
                "Please provide a valid database path (db)."
            )
    else:
        if isinstance(db, str):
            __temp = []
            __temp.append(db)
            db = __temp.copy()
            del __temp
        elif isinstance(db, list) or isinstance(db, tuple):
            __temp = []
            __temp.extend(db)
            db = __temp.copy()
            del __temp
        if db == []:
            raise sqliteException.PathError(
                "Please provide a valid database path (db)."
            )

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
            raise sqliteException.PathError(
                "Please provide a valid database path (compareWith)."
            )

    if len(db) != len(compareWith):
        raise ValueError(
            "Cannot apply command to the provided data set. Please provide equal db(s) and compareWith(s). Should form a matrix."
        )

    final = []
    for i in range(len(db)):
        try:
            data1 = open(db[i], "rb")
            data2 = open(compareWith[i], "rb")
            data1 = data1.read()
            data2 = data2.read()
        except Exception as e:
            if err:
                raise e

        try:
            final.append(True) if data1 == data2 else final.append(False)
        except Exception as e:
            final.append(False)
            if err:
                raise e
    del data1
    del data2
    return final


def clearDb(db="", err=True):
    """
    Clears the database provided.
    """
    if not db:
        db = constants.__dbSqlite__
        if isinstance(db, str):
            db = []
            db.append(constants.__dbSqlite__)
        elif isinstance(db, list) or isinstance(db, tuple):
            db = []
            db.extend(constants.__dbSqlite__)
        if db == []:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        if isinstance(db, str):
            __temp = []
            __temp.append(db)
            db = __temp.copy()
            del __temp
        elif isinstance(db, list) or isinstance(db, tuple):
            __temp = []
            __temp.extend(db)
            db = __temp.copy()
            del __temp
        if db == []:
            raise sqliteException.PathError("Please provide a valid database path.")

    final = []
    for database in db:
        try:
            open(database, "w").write("")
            final.append(True)
        except Exception as e:
            final.append(False)
            if err:
                raise e

    return final


class Database:
    """
    Class for database configuration related operations.
    """

    def __init__(self, db=""):
        self.db = db

    def __str__(self):
        return f'Databases: {", ".join(constants.__dbSqlite__)}'

    def path(self):
        """
        Returns the path of all the connected databases.
        """
        return constants.__dbSqlite__

    def add(self, db=""):
        """
        Adds the database in the connect scope (Connects the database).
        """
        db = db if len(db) != 0 else self.db

        raise sqliteException.DatabaseError(
            'Invalid path input. Path should be a "str" or "list" type object.'
        ) if len(db) == 0 else True

        if isinstance(db, list) or isinstance(db, tuple):
            constants.__dbSqlite__.extend(db)
        elif isinstance(db, str):
            constants.__dbSqlite__.append(db)
        elif isinstance(db, dict):
            raise ValueError(
                "Invalid value provided. Path must be of type list, tuple or str"
            )

    def remove(self, db):
        """
        Removes the database from connect scope (disconnects the database).
        """
        if isinstance(db, list) or isinstance(db, tuple):
            constants.__dbSqlite__ = [
                path for path in constants.__dbSqlite__ if path not in db
            ]
        elif isinstance(db, str):
            constants.__dbSqlite__.remove(db)
        elif isinstance(db, dict):
            raise ValueError(
                "Invalid value provided. Path must be of type list, tuple or str"
            )

    def clear(self):
        """
        Clears the database operation scope i.e. disconnect all the connected databases.
        """
        constants.__dbSqlite__ = []

    def validate(self, db=""):
        """
        Validates whether the database is valid for opeations.
        """
        db = db if len(db) != 0 else self.db

        db = constants.__dbSqlite__ if len(db) == 0 else True

        return advTools.validate(db)

    def getInfo(self, db):
        """
        Returns the information about the database.
        """
        pass


if __name__ == "__main__":
    print("Simple functions extention for SQL-Tools library.")
    print("Helps to perform simple operations related to location of database(s).")
