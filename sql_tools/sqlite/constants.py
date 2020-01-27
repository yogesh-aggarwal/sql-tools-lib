"""
File for storing constant data for sql-tools library.
"""

from sqlite3 import sqlite_version

# Data
__path__ = None
__status__ = None
__jsonFormat__ = "Error in JSON file or it may not in the specified format.\nFormat:\n\t{\n\t\t<database>:\n\t\t{\n\t\t\t<table name>:\n\t\t\t[\n\t\t\t\t<commands>\n\t\t\t]\n\t\t}\n\t}"


# Global constants
__version__ = f"SQL-Tools (sqlite) version: {sqlite_version}"

if __name__ == "__main__":
    print("Contants for SQL-Tools (sqlite) library.")
