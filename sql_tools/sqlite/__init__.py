
from . import driver
from . import constants
from .advTools import *
from .connect import *
from .databFuncs import *
from .execute import *
from .fetch import *
from .io import tableToCSV

__version__ = f"SQL-Tools: SQLite version: {driver.sqlite_version}"
__help__ = 'Visit the documentation for more help or type "help(sql_tools)"'


if __name__ == "__main__":
    print("Welcome to sqlite support module.")
    print("Sub module for sqlite support with sql-tools package.")
    print("Import it from the sql_tools to use it.")
    print("Thanks for using it.")
