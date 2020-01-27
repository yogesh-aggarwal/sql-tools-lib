"""
Execute extension for SQL-Tools library.
"""


import sql_tools.internals as tools
from sql_tools import constants, exception

from . import driver

# TODO: Implement the remaining parameters


def execute(
    command,
    db=[],
    # matrix=True,
    # inlineData=False,
    # splitByColumns=False,
    # asyncExec=False,
    # splitExec=False,
    returnDict=False,
    verbose=False,
    err=True,
    simplify=False,
):
    obj = tools.Exec(command, db, returnDict, verbose, err, simplify)
    obj._Exec__setParams(driver, "mysql", (constants.__credentials__))
    obj.execute()
    return obj
