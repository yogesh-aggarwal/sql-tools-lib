import sqlite3
from numpy import array

__version__ = "SQL Tools version 0.1.9"
__sqliteVersion__ = f"SQL Tools: Sqlite3 version {sqlite3.sqlite_version}"
__sqliteFunctions__ = array(["Create database: Sqlite3.createDatabase()",
                             "Move database: Sqlite3.moveDatabase()",
                             "Copy database: Sqlite3.copyDatabase()",
                             "Delete database: Sqlite3.delDatabase()",
                             "Get no. of records in table(s): Sqlite3.getNoOfRecords()",
                             "Get no. of columns in table(s): Sqlite3.getNoOfColumns()",
                             "Get no. of table(s) in database(s): Sqlite3.getNoOfTables()",
                             "Get columns names of table(s): Sqlite3.getColumnNames()",
                             "Get table names of database(s): Sqlite3.getTableNames()",
                             "Get table(s) creation command: Sqlite3.getTableCommand()"])
__help__ = "Visit the documentation for more help or type \"help(sql_tools)\""

if __name__ == "__main__":
    print("Welcome to sqlite support module.")
    print("Sub module for sqlite support with sql-tools package.")
    print("Import it from the sql_tools to use it.")
    print("Thanks for using it.")
    input()
