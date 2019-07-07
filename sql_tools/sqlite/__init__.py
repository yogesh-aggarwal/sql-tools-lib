import sqlite3

from numpy import array

from . import constants
from .connect import *
from .constants import *
from .databFuncs import *
from .execute import execute
from .fetch import *
# from .advTools import *

__version__ = "SQL Tools version 0.1.9"
__sqliteVersion__ = f"SQL Tools: sqlite version {sqlite3.sqlite_version}"
__sqliteFunctions__ = array(["Create database: sqlite.createDatabase()",
                             "Move database: sqlite.moveDatabase()",
                             "Copy database: sqlite.copyDatabase()",
                             "Delete database: sqlite.delDatabase()",
                             "Get no. of records in table(s): sqlite.getNoOfRecords()",
                             "Get no. of columns in table(s): sqlite.getNoOfColumns()",
                             "Get no. of table(s) in database(s): sqlite.getNoOfTables()",
                             "Get columns names of table(s): sqlite.getColumnNames()",
                             "Get table names of database(s): sqlite.getTableNames()",
                             "Get table(s) creation command: sqlite.getTableCommand()"])
__help__ = "Visit the documentation for more help or type \"help(sql_tools)\""


__path__ = constants.__databPath__  # NOT WORKING -> USING CATCHED VALUE AT START
__time__ = constants.__time__  # NOT WORKING -> USING CATCHED VALUE AT START


if __name__ == "__main__":
    print("Welcome to sqlite support module.")
    print("Sub module for sqlite support with sql-tools package.")
    print("Import it from the sql_tools to use it.")
    print("Thanks for using it.")
    input()
