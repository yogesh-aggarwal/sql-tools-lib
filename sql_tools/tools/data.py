"""
Data related operations for SQL-Tools library.
"""

from . import toolsException
import numpy as np


def dataType(data):
    try:
        dtype = np.array(data).dtype
        if dtype == "O" or dtype == "<U1" or dtype == "<U7" or dtype == "<U11" or dtype == "<U21":
            return "TEXT"
        elif dtype == "int64":
            return "INT"
        else:
            return "Null"

    except Exception:
        raise toolsException.DataError("Invalid data")
