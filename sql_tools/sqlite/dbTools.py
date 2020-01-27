"""
Database operation extension for SQL-Tools library.
"""

import os

import sql_tools.internals as tools


def createDb(db="", err=True):
    """
    Creates the databases at the path provided.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    if not db:
        return False
    else:
        for db in tools.parseDbs(db, base="sqlite"):
            try:
                open(db, "w+").close()
            except Exception as e:
                if err:
                    raise e
    return True


def moveDb(newPath, oldPath="", err=True):
    """
    Moves the database from the old path to new path.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    newPath = tools.parseDbs(newPath, default=False)
    oldPath = tools.parseDbs(oldPath, default=False)
    if not newPath or not oldPath:
        return False
    else:
        for path in tuple(zip(oldPath, newPath)):
            try:
                os.rename(path[0], path[1])
            except Exception as e:
                if err:
                    raise e
    return True


def copyDb(newPath, oldPath="", err=True):
    """
    Creates a copy of database from the old path to the new path.
    Caution:
    ---
    Provide the name of the database in the path.
    """
    newPath = tools.parseDbs(newPath, default=False)
    oldPath = tools.parseDbs(oldPath, default=False)
    if not newPath or not oldPath:
        return False
    else:
        for path in tuple(zip(oldPath, newPath)):
            try:
                file = open(path[0])
                data1 = file.read()
                file.close()
                file = open(path[1], "w+")
                file.write(data1)
                file.close()
            except Exception as e:
                if err:
                    raise e
    return True


def delDb(db="", err=True):
    """
    Deletes the database at the provided path.
    Caution:
    ---
    This action is irreversible.
    """
    if not db:
        return False
    else:
        for db in tools.parseDbs(db, base="sqlite"):
            try:
                os.remove(db)
            except Exception as e:
                if err:
                    raise e
    return True


def isIdentical(db="", err=True):
    """
    Returns whether the database(s) are identical or not.
    """
    result = []
    if not db:
        return False
    else:
        for db in tools.parseDbs(db, base="sqlite"):
            try:
                with open(db, "rb") as f:
                    data = f.read()
                    result.append(data) if data not in result else False
            except Exception as e:
                if err:
                    raise e
    return True if len(result) == 1 else False


def clearDb(db="", err=True):
    """
    Clears the database provided.
    """
    for x in tools.parseDbs(db, base="sqlite"):
        try:
            with open(x, "w") as f:
                f.write("")
        except Exception as e:
            if err:
                raise e
