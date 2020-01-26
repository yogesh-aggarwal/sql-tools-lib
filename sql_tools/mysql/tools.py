import logging
from datetime import datetime
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
        constants.__startTime__ = datetime.now()
        setStatus("Starting execution", logConsole=logConsole)
    elif method == "stop":
        constants.__stopTime__ = datetime.now()
        setStatus("Calculating time", logConsole=logConsole)
    elif method == "result":
        constants.__time__ = (
            f"Wall time: {(constants.__stopTime__ - constants.__startTime__).total_seconds()}s"
        )
        return constants.__time__


def parseDbs(db):
    return execute.execute([], db, _execute__execMethod=False)._execute__parseDatabase()


def parseTables(tables, db):
    # print(tables)
    return execute.execute(tables, db, _execute__execMethod=False)._execute__parseCommands()
