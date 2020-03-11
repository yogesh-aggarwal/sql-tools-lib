"""
Execute extension for SQL-Tools library.
"""

from threading import Thread

import sql_tools.internals as tools
import time
from sql_tools import constants

from . import driver as sqDriver

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
        obj._Exec__setParams(sqDriver, "sqlite")
        obj.execute()
        # time.sleep(.0000001)
        constants.__result__ = obj
        callback(obj) if callback else False
        return obj

    if asyncExec:
        t = Thread(target=run)
        t.start()
    else:
        return run()
