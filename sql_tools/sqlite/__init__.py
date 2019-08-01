import sqlite3

from . import constants
from .advTools import *
from .connect import connect
from .databFuncs import *
from .execute import *
from .fetch import *
from .io import tableToCSV

__version__ = f"SQL-Tools: SQLite version: {sqlite3.sqlite_version}"
__help__ = "Visit the documentation for more help or type \"help(sql_tools)\""


# __path__ = constants.__databPath__  # NOT WORKING -> USING CATCHED VALUE AT START
# __time__ = constants.__time__  # NOT WORKING -> USING CATCHED VALUE AT START

def __path__():
    return constants.__databPath__

def __time__():
    return constants.__time__


if __name__ == "__main__":
    print("Welcome to sqlite support module.")
    print("Sub module for sqlite support with sql-tools package.")
    print("Import it from the sql_tools to use it.")
    print("Thanks for using it.")
    input()
