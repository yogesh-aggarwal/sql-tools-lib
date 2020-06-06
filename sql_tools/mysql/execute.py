"""
Execute extension for SQL-Tools library.
"""


from threading import Thread

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
    # splitExec=False,
    returnDict=False,
    verbose=False,
    err=True,
    simplify=False,
    asyncExec=False,
    callback=False,
):
    def run():
        obj = tools.Exec(command, db, returnDict, verbose, err, simplify)
        obj._Exec__setParams(driver, "mysql", (constants.__credentials__))
        obj.execute()
        constants.__result__ = obj
        callback(obj) if callback else False
        return obj

    if asyncExec:
        t = Thread(target=run)
        t.start()
    else:
        return run()
