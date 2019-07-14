import sqlite3

from . import constants
from .connect import connect
from .databFuncs import createDatabase, moveDatabase, copyDatabase, delDatabase, isIdentical, clearDatabase
from .execute import execute
from .fetch import getNoOfRecords, getNoOfColumns, getColumnNames, getTableNames, getTableCommand, getDatabaseSize, getSampleDatabase

# from .advTools import *

__version__ = f"SQL-Tools: SQLite version: {sqlite3.sqlite_version}"
__help__ = "Visit the documentation for more help or type \"help(sql_tools)\""


# __path__ = constants.__databPath__  # NOT WORKING -> USING CATCHED VALUE AT START
# __time__ = constants.__time__  # NOT WORKING -> USING CATCHED VALUE AT START

def __path__():
    return constants.__databPath__

def __time__():
    return constants.__time__
