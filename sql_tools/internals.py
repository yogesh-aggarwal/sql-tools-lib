import datetime
import logging
from os import getpid

from sql_tools import constants

from . import exception


def setStatus(arg, raiseError=True, verbose=False):
    try:
        if verbose:
            logging.basicConfig(format="[%(process)d] SQL-Tools: %(message)s")
            logging.warning(arg)  # Change the logging style
            constants.__pid__ = getpid()
        else:
            constants.__history__.append(arg)
        constants.__status__ = arg
        return True
    except Exception:
        if raiseError:
            raise exception.DatabaseError("Unable to set the status.")
        return False


def checkInstance(value, *args):
    return True if [x for x in args if isinstance(value, x)] else False


def timer(method, execMethod=True, verbose=False):
    if method == "start":
        constants.__startTime__ = datetime.now()
        setStatus("Starting execution", verbose=verbose)
    elif method == "stop":
        constants.__stopTime__ = datetime.now()
        setStatus("Calculating time", verbose=verbose)
    elif method == "result":
        constants.__time__ = f"Wall time: {(constants.__stopTime__ - constants.__startTime__).total_seconds()}s"
        return constants.__time__
