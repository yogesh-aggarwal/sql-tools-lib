__help__ = """
Please import the module from any file and use it to simplify your work.
For more help contact on GitHub repository https://github.com/yogesh-aggarwal/sql-tools-lib
You can read the full documentation on https://yogesh-aggarwal.gitbook.io/sql-tools/

Thanks for using the package.
"""

__version__ = "SQL-Tools version: 3.0.1"

# &Global Sharing
__time__ = None
__startTime__ = None
__stopTime__ = None
__query__ = None
__status__ = None
__history__ = []

# &Global constant variables
__pid__ = None
__jsonFormat__ = "Error in JSON file or it may not in the specified format.\nFormat:\n\t{\n\t\t<database>:\n\t\t{\n\t\t\t<table name>:\n\t\t\t[\n\t\t\t\t<commands>\n\t\t\t]\n\t\t}\n\t}"


# &MySQL
__dbMysql__ = []
__credentials__ = None
__charset__ = "utf8mb4"
simplify = False
dtypeMap = {
    "float64": "FLOAT",
    "float32": "FLOAT",
    "int64": "INT(64)",
    "int32": "INT(32)",
    "object": "TEXT",
    "datetime64[ns]": "DATE",
    "datetime32[ns]": "DATE",
}

# &SQLite
__dbSqlite__ = []
