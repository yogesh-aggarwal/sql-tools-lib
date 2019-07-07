"""
Execute extension for SQL-Tools library.
"""

import json
import sqlite3
import time

import numpy as np

from . import __tools, constants, sqliteException


def execute(command="", databPath="", matrix=True, inlineData=False, splitByColumns=False, pathJSON=False, logConsole=False, __execMethod=True, __commit=True):
    """
    Executes the given command to the specified database(s).
    Attributes
    ====
    command
    ---
    The command to be executed. Can execute multiple commands for multiple databases accordingly. Provide a list containing the command for multiple databases.

    databPath
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
    """
    if __execMethod:
        constants.__startTime__ = time.time()
        __tools.setStatus("Starting execution", logConsole=logConsole)

    if not databPath:
        if pathJSON:
            databPath = []
            command = []
            with open(pathJSON, "r") as f:
                try:
                    data = json.loads(f.read())
                except Exception:
                    raise sqliteException.JSONError("JSON file error. Could be the syntax problem.")
            keys = data.keys()
            for i in keys:
                for j in data[i]:
                    command.append(data[i][j][0])
                databPath.append(i)
        else:
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
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        databPath = __temp_lst__.copy()
        del __temp_lst__

    # For command to list
    __temp_lst__ = []
    __temp_lst__.append(command)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise sqliteException.CommandError("Invalid command input. Command should be a \"str\" or \"list\" type object.")
    command = __temp_lst__.copy()

    # For database to list
    __temp_lst__ = []
    __temp_lst__.append(databPath)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
    databPath = __temp_lst__.copy()

    try:
        if len(command) != len(databPath):
            raise sqliteException.MatrixError("Cannot apply command to the provided data set. Please provide equal commands and paths. Should form a matrix.")
    except TypeError:
        pass
    del __temp_lst__

    data = []
    for i in range(len(databPath)):
        conn = sqlite3.connect(databPath[i])
        c = conn.cursor()
        try:
            c.execute(command[i])
        except Exception as e:
            raise sqliteException.QueryError(f"ERROR IN SQL QUERY ---> {e} (From database {databPath[i]})")
        result = []
        try:
            for data_fetched in c.fetchall():
                result.append(data_fetched)
        except Exception as e:
            raise sqliteException.UnknownError(f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {databPath[i]})")
        conn.commit()
        c.close()
        conn.close()
        data.append(result)

    if __execMethod:
        constants.__stopTime__ = time.time()
        constants.__time__ = f"Wall time: {constants.__stopTime__ - constants.__startTime__}s"

    # FOR INLINE DATA
    inlineData = False
    __temp = []
    if inlineData:
        if __execMethod:
            __tools.setStatus("Inlining data", logConsole=logConsole)
        for values in data:
            for value in values:
                __temp.append(value)
        data = __temp.copy()
    del __temp

    # FOR SPLITBYCOLUMNS
    __temp = []
    if splitByColumns:
        if __execMethod:
            __tools.setStatus("Spliting by columns", logConsole=logConsole)
        __temp = []
        for database in data:
            __temp.append(list(zip(*database)))
        data = __temp.copy()
    del __temp

    # FOR MATRIX
    if matrix:
        if __execMethod:
            __tools.setStatus("Converting to matrix", logConsole=logConsole)
            __tools.setStatus("Returning results", logConsole=logConsole)
        return np.array(data)
    else:
        if __execMethod:
            __tools.setStatus("Returning results", logConsole=logConsole)
        return data


if __name__ == "__main__":
    print("Execute extension for SQL-Tools library.")
    print("Note: It can be used seperately to save memory rather than to import full library.\n\t* Provide database name if used seperately.")
    input()
