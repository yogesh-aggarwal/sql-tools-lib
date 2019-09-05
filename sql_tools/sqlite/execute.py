"""
Execute extension for SQL-Tools library.
"""

import json
from . import driver
import time
import os

import numpy as np

from . import __tools, constants, sqliteException


def execute(
    command="",
    databPath="",
    matrix=True,
    inlineData=False,
    splitByColumns=False,
    pathJSON=False,
    asyncExec=False,
    splitExec=False,
    returnDict=False,
    logConsole=False,
    raiseError=True,
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

    pathJSON
    ---
    Whether to fetch the commands from a `json` file.

    logConsole
    ---
    Whether to log the steps to the console or not.

    commit
    ---
    Whether to commit the changes immidiately after excution of each command. It is recommended to use it when you have run multiple commands on the same database at the same time.
    """
    constants.__processId__ = os.getpid()
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
                    if raiseError:
                        raise sqliteException.JSONError(
                            "JSON file error. Could be the syntax problem."
                        )
                        exit(1)
                    __tools.setStatus(
                        "JSON file error. Could be the syntax problem.", logConsole=True
                    )
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
                if raiseError:
                    raise sqliteException.PathError(
                        "Please provide a valid database path."
                    )
                    exit(1)
                __tools.setStatus(
                    "Please provide a valid database path.", logConsole=True1
                )
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            if raiseError:
                raise sqliteException.PathError(
                    'Invalid path input. Path should be a "str" or "list" type object.'
                )
                exit(1)
            __tools.setStatus(
                'Invalid path input. Path should be a "str" or "list" type object.',
                logConsole=True,
            )
        databPath = __temp_lst__.copy()
        del __temp_lst__

    # For command to list
    try:
        __temp_lst__ = []
        __temp_lst__.append(command)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            if raiseError:
                raise sqliteException.CommandError(
                    'Invalid command input. Command should be a "str" or "list" type object.'
                )
                exit(1)
            __tools.setStatus(
                'Invalid command input. Command should be a "str" or "list" type object.',
                logConsole=True,
            )
        command = __temp_lst__.copy()
    except Exception:
        if raiseError:
            raise sqliteException.CommandError("Error while parsing your command.")
            exit(1)
        __tools.setStatus("Error while parsing your command.", logConsole=True)

    # For database to list
    try:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            if raiseError:
                raise sqliteException.PathError(
                    'Invalid path input. Path should be a "str" or "list" type object.'
                )
                exit(1)
            __tools.setStatus(
                'Invalid path input. Path should be a "str" or "list" type object.',
                logConsole=True,
            )
        databPath = __temp_lst__.copy()
        if len(databPath) > 1:
            splitExec = False
    except Exception:
        if raiseError:
            raise sqliteException.PathError("Error while parsing your path.")
            exit(1)
        __tools.setStatus("Error while parsing your path.", logConsole=True)

    # Unequal condition
    try:
        if len(command) != len(databPath):
            if raiseError:
                raise sqliteException.MatrixError(
                    "Cannot apply command to the provided data set. Please provide equal commands and paths. Should form a matrix."
                )
                exit(1)
            __tools.setStatus(
                "Cannot apply command to the provided data set. Please provide equal commands and paths. Should form a matrix.",
                logConsole=True,
            )

    except TypeError:
        pass
    del __temp_lst__

    # Executing the main command
    data = []
    if splitExec:
        __tools.setStatus(
            "Opted for splitExec (seperate execution)", logConsole=logConsole
        )
        for i in range(len(databPath)):
            conn = driver.connect(databPath[i])
            __tools.setStatus("Connected", logConsole=logConsole)
            c = conn.cursor()
            __tools.setStatus("Creating the pointer", logConsole=logConsole)

            try:
                __tools.setStatus(
                    f"Executing [{i}]: {command[i]} ({databPath[i]})",
                    logConsole=logConsole,
                )
                c.execute(command[i])
            except Exception as e:
                if raiseError:
                    raise sqliteException.QueryError(
                        f"ERROR IN SQL QUERY ---> {e} (From database {databPath[i]})"
                    )
                    exit(1)
                __tools.setStatus(
                    f"ERROR IN SQL QUERY ---> {e} (From database {databPath[i]})",
                    logConsole=True,
                )

            result = []
            try:
                for data_fetched in c.fetchall():
                    result.append(data_fetched)
                    __tools.setStatus(
                        f"Fetched data ({databPath[i]})", logConsole=logConsole
                    )
            except Exception as e:
                if raiseError:
                    raise sqliteException.UnknownError(
                        f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {databPath[i]})"
                    )
                    exit(1)
                __tools.setStatus(
                    f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {databPath[i]})",
                    logConsole=True,
                )
            if commit:
                conn.commit()
                __tools.setStatus("Changes commited", logConsole=logConsole)
                c.close()
                conn.close()
            data.append(result)
    else:
        conn = driver.connect(databPath[0])
        __tools.setStatus("Connected", logConsole=logConsole)
        c = conn.cursor()
        __tools.setStatus("Created pointer", logConsole=logConsole)

        for i in range(len(databPath)):
            try:
                __tools.setStatus(
                    f"Executing [{i}]: {command[i]} ({databPath[i]})",
                    logConsole=logConsole,
                )
                c.execute(command[i])
            except Exception as e:
                if raiseError:
                    raise sqliteException.QueryError(
                        f"ERROR IN SQL QUERY ---> {e} (From database {databPath[i]})"
                    )
                    exit(1)
                __tools.setStatus(
                    f"ERROR IN SQL QUERY ---> {e} (From database {databPath[i]})",
                    logConsole=True,
                )
            result = []
            try:
                for data_fetched in c.fetchall():
                    result.append(data_fetched)
                __tools.setStatus(
                    f"Fetched [{i}] ({databPath[i]})", logConsole=logConsole
                )
            except Exception as e:
                if raiseError:
                    raise sqliteException.UnknownError(
                        f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {databPath[i]})"
                    )
                    exit(1)
                __tools.setStatus(
                    f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {databPath[i]})",
                    logConsole=True,
                )
            data.append(result)
        if commit:
            conn.commit()
            __tools.setStatus("Changes commited", logConsole=logConsole)
            c.close()
            conn.close()
            __tools.setStatus("Connection closed", logConsole=logConsole)

    __tools.setStatus("Preparing results", logConsole=logConsole)

    # Conditions
    if __execMethod:
        constants.__stopTime__ = time.time()
        __tools.setStatus("Calculating time", logConsole=logConsole)
        constants.__time__ = (
            f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
        )

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
        __temp__ = np.array(data)
        if __execMethod:
            __tools.setStatus(
                f"Converting to matrix [{__temp__.shape[0]}x{__temp__.shape[1]}]",
                logConsole=logConsole,
            )
            __tools.setStatus("Returning results", logConsole=logConsole)
        if returnDict:
            return dict(zip(databPath, np.array(data)))
        else:
            return np.array(data)
    else:
        if __execMethod:
            __tools.setStatus("Returning results", logConsole=logConsole)
        if returnDict:
            return dict(zip(databPath, data))
        else:
            return data


def commit(databPath=""):
    """
    Commits the changes to the database if `commit=False` is provided while executing the commands.
    """
    if isinstance(databPath, str):
        execute()
    else:
        raise ValueError("Please provide a valid database path. It should be string.")
        exit(1)


if __name__ == "__main__":
    print("Execute extension for SQL-Tools library.")
    print(
        "Note: It can be used seperately to save memory rather than to import full library.\n\t* Provide database name if used seperately."
    )
