"""
File for storing constant data for sql-tools library.
"""

# Data
__path__ = None
__time__ = None
__startTime__ = None
__stopTime__ = None
__status__ = None
__jsonFormat__ = "Error in JSON file or it may not in the specified format.\nFormat:\n\t{\n\t\t<database>:\n\t\t{\n\t\t\t<table name>:\n\t\t\t[\n\t\t\t\t<commands>\n\t\t\t]\n\t\t}\n\t}"
__history__ = []


# FOR "execute.py"
__databPath__ = []

# FOR "databFuncs.py"
__copyCount__ = 0

if __name__ == "__main__":
    print("Contants for SQL-Tools library.")
    input()
