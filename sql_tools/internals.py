import logging
import os
import warnings
from datetime import datetime

from numpy import array
from sql_tools import constants

from . import exception

warnings.simplefilter(action="ignore", category=FutureWarning)


"""
& Available functions
- setStatus
- checkInstance
- timer
- parseDbs
- parseTables
- __tableToCsv
- dataType

& Available tools
- Exec
"""


def setStatus(arg, err=True, verbose=False):
    try:
        if verbose:
            logging.basicConfig(format="[%(process)d] SQL-Tools: %(message)s")
            logging.warning(arg)  # Change the logging style
            constants.__pid__ = os.getpid()
        constants.__status__ = arg
        return True
    except Exception:
        if err:
            raise exception.DatabaseError("Unable to set the status.")
        return False


def checkInstance(value, *args):
    return True if [x for x in args if isinstance(value, x)] else False


def timer(main):
    def wrapper(classObj):
        constants.__startTime__ = datetime.now()
        setStatus("Starting execution")

        result = main(classObj)

        constants.__stopTime__ = datetime.now()
        setStatus("Calculating time")

        constants.__time__ = f"Wall time: {(constants.__stopTime__ - constants.__startTime__).total_seconds()}s"
        return result

    return wrapper


def parseDbs(db, base="mysql", default=True):
    if not db:
        if default:
            if base == "mysql":
                db = constants.__dbMysql__
            if base == "sqlite":
                db = constants.__dbSqlite__
            elif base == "json":
                constants.__dbJson__
            else:
                exception.Unknown("Invalid database base provided")
    return Exec([], db)._Exec__parseDatabase()


def parseTables(tables, db):
    return Exec(tables, db)._Exec__parseCommands()


# &SQLite
def __tbToCsv(data, tblName, db="", tbl=True, database=True, index=False):
    # db = parseDbs(db)

    # db = db[
    #     0
    # ]  # REMOVE THIS FOR MULTIPLE DATABASES AS IT WILL FETCH THE FIRST DATABASE ONLY

    # columns = fetch.getCNames(tblName, db=db)[0]
    # if tbl and database:
    #     if index != False:
    #         DataFrame(data, columns=columns, index=index).to_csv(
    #             f"{path.basename(db)}.{tblName}.csv"
    #         )
    #     else:
    #         DataFrame(data, columns=columns).to_csv(
    #             f"{path.basename(db)}.{tblName}.csv", index=False
    #         )
    # elif tbl:
    #     if index != False:
    #         DataFrame(data, columns=columns, index=index).to_csv(f"{tblName}.csv")
    #     else:
    #         DataFrame(data, columns=columns).to_csv(f"{tblName}.csv", index=False)

    # # else:
    # #     raise AttributeError("One attribute must be provided.")
    return True


def dataType(data):
    try:
        dtype = array(data).dtype
        if (
            dtype == "O"
            or dtype == "<U1"
            or dtype == "<U5"
            or dtype == "<U6"
            or dtype == "<U7"
            or dtype == "<U11"
            or dtype == "<U21"
        ):
            return "str"
        elif dtype == "int64" or dtype == "<U2":
            return "int"
        else:
            return "None"

    except Exception:
        pass


class Exec:
    """Base exec class for execution operations with SQLite & MySQL"""

    def __init__(
        self,
        command,
        db=[],
        returnDict=False,
        verbose=False,
        err=True,
        simplify=False,
    ):
        self.__command = command  # Commands to be executed
        self.db = db  # Database
        self.__returnDict = returnDict
        self.__verbose = verbose  # For being verbose
        self.__raiseError = err  # Raise error or not if something goes wrong
        self.__simplify = simplify  # Commit the changes
        self.__result = []
        self._driver = None
        self.base = "mysql"
        self.property = ()
    
    def __repr__(self):
        return self.get

    @property
    def get(self):
        return self.__result

    @property
    def list(self):
        return self.__result.tolist()

    @property
    def dict(self):
        return dict(zip(self.db, self.__result))

    @property
    def time(self):
        return constants.__time__

    def __properties(self, db):
        if self.property:
            host, user, pword = self.property[0]
        return (
            self._driver.connect(
                host=host,
                user=user,
                password=pword,
                db=db,
                charset=constants.__charset__,
            )
            if self.property
            else self._driver.connect(db)
        )

    def __setParams(self, driver, type, *args):
        self._driver = driver
        self.base = type
        self.property = () if type == "sqlite" else args

    @timer
    def execute(self):
        self.__simplify = constants.simplify if not self.__simplify else self.__simplify
        constants.__pid__ = os.getpid()

        # &For database to list
        self.db = self.__parseDatabase()

        # &For command to list
        self.__command = self.__parseCommands()

        # &Unequal condition
        if len(self.__command) != len(self.db):
            raise exception.UnknownError(
                "Database and commands are not commuting, n(commands) != n(database)"
            )

        # &Executing the main command
        # Executing commands database wise
        final = []
        for i in range(len(self.db)):
            # Connect to the database
            connection = self.__properties(self.db[i])
            setStatus("Connected", verbose=self.__verbose)
            setStatus("Creating the pointer", verbose=self.__verbose)

            data = []
            setStatus(
                f"Executing command database: {self.db[i]}", verbose=self.__verbose
            )
            for comm in self.__command[i]:
                constants.__history__.append(comm)
                try:
                    cursor = connection.cursor()
                    cursor.execute(comm)
                    data.append(list(cursor.fetchall()))
                except Exception as e:
                    if self.__raiseError:
                        raise exception.QueryError(e)
                    return False

            final.append(data)

            connection.commit()
            connection.close()

        setStatus("Preparing results", verbose=self.__verbose)

        final = array(final)  # For performace
        if self.__simplify:
            if len(self.db) == 1:
                final = final[0]

        self.__result = final

        # &Return dictionary
        if self.__returnDict:
            self.__result = self.dict

    def __parseCommands(self, command="", db=""):
        command = command if command else self.__command
        setStatus("Parsing commands", self.__raiseError, self.__verbose)
        try:
            db = parseDbs(db) if db else parseDbs(self.db)
            if len(db) == 1:
                if checkInstance(command, str):
                    command = [[command]]
                elif checkInstance(command[0], str):
                    command = [command]
                elif checkInstance(command[0], list, tuple):
                    if checkInstance(command[0][0], list, tuple):
                        raise exception.UnknownError(
                            "Database and commands are not commuting, n(commands) != n(database)"
                        )
                else:
                    raise exception.CommandError()
            else:
                if checkInstance(command, list, tuple):
                    if not checkInstance(
                        command[0], list, tuple
                    ) or checkInstance(command[0][0], list, tuple):
                        raise exception.UnknownError(
                            "Database and commands are not commuting, n(commands) != n(database)"
                        )
                else:
                    raise exception.CommandError()
        except Exception:
            if self.__raiseError:
                raise exception.CommandError("Command not provided! Commands must be a str or list object")
        return command

    def __parseDatabase(self, db=""):
        db = db if db else self.db
        setStatus("Parsing database(s)", self.__raiseError, self.__verbose)
        try:
            if checkInstance(db, str):
                db = [db]
            elif not checkInstance(db, list, tuple) or not checkInstance(
                db[0], str
            ):
                raise ValueError
        except Exception:
            pass

        if not db:
            db = (
                constants.__dbSqlite__
                if self.base == "sqlite"
                else constants.__dbMysql__
            )
        if not db:
            if self.__raiseError:
                raise exception.DatabaseError()
            else:
                setStatus("Error while parsing databases", verbose=True)
        return db
