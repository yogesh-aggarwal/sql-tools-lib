"""
Execute extension for SQL-Tools library.
"""

import json
import os
import time

import numpy as np

import sql_tools.internals as tools
from sql_tools import constants

from . import driver, sqliteException


def execute(
    command="",
    db="",
    matrix=True,
    inlineData=False,
    splitByColumns=False,
    pathJSON=False,
    asyncExec=False,
    splitExec=False,
    returnDict=False,
    verbose=False,
    err=True,
    commit=True,
    __execMethod=True,
):
    """
    Executes the given command to the specified database(s).
    Attributes
    ====
    command
    ---
    The command to be executed. Can execute multiple commands for multiple databases accordingly. Provide a list containing the command for multiple databases.

    db
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

    pathJSON
    ---
    Whether to fetch the commands from a `json` file.

    verbose
    ---
    Whether to log the steps to the console or not.

    commit
    ---
    Whether to commit the changes immidiately after excution of each command. It is recommended to use it when you have run multiple commands on the same database at the same time.
    """
    constants.__pid__ = os.getpid()
    if __execMethod:
        constants.__startTime__ = time.time()
        tools.setStatus("Starting execution", verbose=verbose)

    if not db:
        if pathJSON:
            db = []
            command = []
            with open(pathJSON, "r") as f:
                try:
                    data = json.loads(f.read())
                except Exception:
                    if err:
                        raise sqliteException.JSONError(
                            "JSON file error. Could be the syntax problem."
                        )
                        exit(1)
                    tools.setStatus(
                        "JSON file error. Could be the syntax problem.", verbose=True
                    )
            keys = data.keys()
            for i in keys:
                for j in data[i]:
                    command.append(data[i][j][0])
                db.append(i)

        else:
            db = constants.__dbSqlite__
            if isinstance(db, str):
                db = []
                db.append(constants.__dbSqlite__)
            elif tools.checkInstance(db, list, tuple):
                db = []
                db.extend(constants.__dbSqlite__)
            if db == []:
                if err:
                    raise sqliteException.PathError(
                        "Please provide a valid database path."
                    )
                    exit(1)
                tools.setStatus("Please provide a valid database path.", verbose=True)
    else:
        __temp_lst__ = []
        __temp_lst__.append(db)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            if err:
                raise sqliteException.PathError(
                    'Invalid path input. Path should be a "str" or "list" type object.'
                )
                exit(1)
            tools.setStatus(
                'Invalid path input. Path should be a "str" or "list" type object.',
                verbose=True,
            )
        db = __temp_lst__.copy()
        del __temp_lst__

    # &For command to list
    try:
        __temp_lst__ = []
        __temp_lst__.append(command)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            if err:
                raise sqliteException.CommandError(
                    'Invalid command input. Command should be a "str" or "list" type object.'
                )
                exit(1)
            tools.setStatus(
                'Invalid command input. Command should be a "str" or "list" type object.',
                verbose=True,
            )
        command = __temp_lst__.copy()
    except Exception:
        if err:
            raise sqliteException.CommandError("Error while parsing your command.")
            exit(1)
        tools.setStatus("Error while parsing your command.", verbose=True)

    # &For database to list
    try:
        __temp_lst__ = []
        __temp_lst__.append(db)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            if err:
                raise sqliteException.PathError(
                    'Invalid path input. Path should be a "str" or "list" type object.'
                )
                exit(1)
            tools.setStatus(
                'Invalid path input. Path should be a "str" or "list" type object.',
                verbose=True,
            )
        db = __temp_lst__.copy()
        if len(db) > 1:
            splitExec = False
    except Exception:
        if err:
            raise sqliteException.PathError("Error while parsing your path.")
            exit(1)
        tools.setStatus("Error while parsing your path.", verbose=True)

    # &Unequal condition
    try:
        if len(command) != len(db):
            if err:
                raise sqliteException.MatrixError(
                    "Cannot apply command to the provided data set. Please provide equal commands and paths. Should form a matrix."
                )
                exit(1)
            tools.setStatus(
                "Cannot apply command to the provided data set. Please provide equal commands and paths. Should form a matrix.",
                verbose=True,
            )

    except TypeError:
        pass
    del __temp_lst__

    # &Executing the main command
    data = []
    if splitExec:
        tools.setStatus("Opted for splitExec (seperate execution)", verbose=verbose)
        for i in range(len(db)):
            conn = driver.connect(db[i])
            tools.setStatus("Connected", verbose=verbose)
            c = conn.cursor()
            tools.setStatus("Creating the pointer", verbose=verbose)

            try:
                tools.setStatus(
                    f"Executing [{i}]: {command[i]} ({db[i]})", verbose=verbose,
                )
                c.execute(command[i])
            except Exception as e:
                if err:
                    raise sqliteException.QueryError(
                        f"ERROR IN SQL QUERY ---> {e} (From database {db[i]})"
                    )
                    exit(1)
                tools.setStatus(
                    f"ERROR IN SQL QUERY ---> {e} (From database {db[i]})",
                    verbose=True,
                )

            result = []
            try:
                for data_fetched in c.fetchall():
                    result.append(data_fetched)
                    tools.setStatus(f"Fetched data ({db[i]})", verbose=verbose)
            except Exception as e:
                if err:
                    raise sqliteException.UnknownError(
                        f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {db[i]})"
                    )
                    exit(1)
                tools.setStatus(
                    f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {db[i]})",
                    verbose=True,
                )
            if commit:
                conn.commit()
                tools.setStatus("Changes commited", verbose=verbose)
                c.close()
                conn.close()
            data.append(result)
    else:
        conn = driver.connect(db[0])
        tools.setStatus("Connected", verbose=verbose)
        c = conn.cursor()
        tools.setStatus("Created pointer", verbose=verbose)

        for i in range(len(db)):
            try:
                tools.setStatus(
                    f"Executing [{i}]: {command[i]} ({db[i]})", verbose=verbose,
                )
                c.execute(command[i])
            except Exception as e:
                if err:
                    raise sqliteException.QueryError(
                        f"ERROR IN SQL QUERY ---> {e} (From database {db[i]})"
                    )
                    exit(1)
                tools.setStatus(
                    f"ERROR IN SQL QUERY ---> {e} (From database {db[i]})",
                    verbose=True,
                )
            result = []
            try:
                for data_fetched in c.fetchall():
                    result.append(data_fetched)
                tools.setStatus(f"Fetched [{i}] ({db[i]})", verbose=verbose)
            except Exception as e:
                if err:
                    raise sqliteException.UnknownError(
                        f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {db[i]})"
                    )
                    exit(1)
                tools.setStatus(
                    f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {db[i]})",
                    verbose=True,
                )
            data.append(result)
        if commit:
            conn.commit()
            tools.setStatus("Changes commited", verbose=verbose)
            c.close()
            conn.close()
            tools.setStatus("Connection closed", verbose=verbose)

    tools.setStatus("Preparing results", verbose=verbose)

    # &Conditions
    if __execMethod:
        constants.__stopTime__ = time.time()
        tools.setStatus("Calculating time", verbose=verbose)
        constants.__time__ = (
            f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
        )

    # &FOR INLINE DATA
    inlineData = False
    __temp = []
    if inlineData:
        if __execMethod:
            tools.setStatus("Inlining data", verbose=verbose)
        for values in data:
            for value in values:
                __temp.append(value)
        data = __temp.copy()
    del __temp

    # &FOR SPLITBYCOLUMNS
    __temp = []
    if splitByColumns:
        if __execMethod:
            tools.setStatus("Spliting by columns", verbose=verbose)
        __temp = []
        for database in data:
            __temp.append(list(zip(*database)))
        data = __temp.copy()
    del __temp

    # &FOR MATRIX
    if matrix:
        __temp__ = np.array(data)
        if __execMethod:
            tools.setStatus(
                f"Converting to matrix [{__temp__.shape[0]}x{__temp__.shape[1]}]",
                verbose=verbose,
            )
            tools.setStatus("Returning results", verbose=verbose)
        if returnDict:
            return dict(zip(db, np.array(data)))
        else:
            return np.array(data)
    else:
        if __execMethod:
            tools.setStatus("Returning results", verbose=verbose)
        if returnDict:
            return dict(zip(db, data))
        else:
            return data


def commit(db=""):
    """
    Commits the changes to the database if `commit=False` is provided while executing the commands.
    """
    if isinstance(db, str):
        execute()
    else:
        raise ValueError("Please provide a valid database path. It should be string.")
        exit(1)


if __name__ == "__main__":
    print("Execute extension for SQL-Tools library.")
    print(
        "Note: It can be used seperately to save memory rather than to import full library.\n\t* Provide database name if used seperately."
    )
