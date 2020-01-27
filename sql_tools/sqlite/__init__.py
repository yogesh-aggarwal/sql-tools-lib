from . import driver
from .advTools import *
from .connect import *
from .dbTools import *
from .execute import *
from .tableTools import *
from .io import *

__version__ = f"SQL-Tools: SQLite version: {driver.sqlite_version}"


if __name__ == "__main__":
    print("Welcome to sqlite support module.")
    print("Sub module for sqlite support with sql-tools package.")
    print("Import it from the sql_tools to use it.")
    print("Thanks for using it.")
