import logging
import time
from os import getpid

from . import constants, mysqlException, execute


def setStatus(arg, raiseError=True, logConsole=False):
    try:
        if logConsole:
            logging.basicConfig(format="[%(process)d] SQL-Tools: %(message)s")
            logging.warning(arg)  # Change the logging style
            constants.__processId__ = getpid()
        else:
            constants.__history__.append(arg)
        constants.__status__ = arg
        return True
    except Exception:
        if raiseError:
            raise mysqlException.UnknownError("Unable to set the status.")
        return False


def checkInstance(value, *args):
    return True if [x for x in args if isinstance(value, x)] else False


def timer(method, execMethod=True, logConsole=False):
    if method == "start":
        constants.__startTime__ = time.time()
        setStatus("Starting execution", logConsole=logConsole)
    elif method == "stop":
        constants.__stopTime__ = time.time()
        setStatus("Calculating time", logConsole=logConsole)
        constants.__time__ = (
            f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
        )
    elif method == "result":
        return constants.__stopTime__ - constants.__startTime__


def parseDbs(db):
    return execute.execute([], db, _execute__execMethod=False)._execute__parseDatabase()


def parseTables(tables, db):
    # print(tables)
    return execute.execute(tables, db, _execute__execMethod=False)._execute__parseCommands()
