"""
File containing methods to fetch data.
"""

import datetime
import json
import logging
import sqlite3
import time

import numpy as np
import pandas as pd
from __tools import *


__history__ = []


def execute( command="", databPath="", matrix=True, inlineData=False, splitByColumns=False, pathJSON=False, logConsole=False, __execMethod=True, __commit=True):
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
        startTime = time.time()
        setStatus("Starting execution", logConsole=logConsole)

    if not databPath:
        if pathJSON:
            databPath = []
            command = []
            with open(pathJSON, "r") as f:
                try:
                    data = json.loads(f.read())
                except Exception:
                    raise ValueError("JSON file error. Could be the syntax problem.")
            keys = data.keys()
            for i in keys:
                for j in data[i]:
                    command.append(data[i][j][0])
                databPath.append(i)
        else:
            databPath = databPath
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        databPath = __temp_lst__.copy()
        del __temp_lst__

    __temp_lst__ = []
    __temp_lst__.append(command)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
    command = __temp_lst__.copy()
    if len(command) != len(databPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal commands and paths. Should form a square matrix.")
    del __temp_lst__

    data = []
    for i in range(len(databPath)):
        conn = sqlite3.connect(databPath[i])
        c = conn.cursor()
        try:
            c.execute(command[i])
        except Exception as e:
            raise ValueError(f"ERROR IN SQL QUERY ---> {e} (From database {databPath[i]})")
        result = []
        try:
            for data_fetched in c.fetchall():
                result.append(data_fetched)
        except Exception as e:
            raise ValueError(f"SQL: SOME ERROR OCCURED.\n---> {e} (From database {databPath[i]})")
        conn.commit()
        c.close()
        conn.close()
        data.append(result)

    if __execMethod:
        stopTime = time.time()
        execTime = stopTime-startTime

    # FOR INLINE DATA
    inlineData = False
    __temp = []
    if inlineData:
        if __execMethod:
            setStatus("Inlining data", logConsole=logConsole)
        for values in data:
            for value in values:
                __temp.append(value)
        data = __temp.copy()
    del __temp


    # FOR SPLITBYCOLUMNS
    __temp = []
    if splitByColumns:
        if __execMethod:
            setStatus("Spliting by columns", logConsole=logConsole)
        __temp = []
        for database in data:
            __temp.append(list(zip(*database)))
        data = __temp.copy()
    del __temp


    # FOR MATRIX
    if matrix:
        if __execMethod:
            setStatus("Converting to matrix", logConsole=logConsole)
            setStatus("Returning results", logConsole=logConsole)
        return np.array(data)
    else:
        if __execMethod:
            setStatus("Returning results", logConsole=logConsole)
        return data




if __name__ == "__main__":
    print(execute("SELECT * FROM STUDENT", databPath="hello.db", logConsole=True))
