import logging
import os
from datetime import datetime
from os import getpid

from numpy import array

import sql_tools.internals as tools
from sql_tools import constants

from . import exception


def setStatus(arg, err=True, verbose=False):
    try:
        if verbose:
            logging.basicConfig(format="[%(process)d] SQL-Tools: %(message)s")
            logging.warning(arg)  # Change the logging style
            constants.__pid__ = getpid()
        else:
            constants.__history__.append(arg)
        constants.__status__ = arg
        return True
    except Exception:
        if err:
            raise exception.DatabaseError("Unable to set the status.")
        return False


def checkInstance(value, *args):
    return True if [x for x in args if isinstance(value, x)] else False


def timer(method, verbose=False):
    if method == "start":
        constants.__startTime__ = datetime.now()
        setStatus("Starting execution", verbose=verbose)
    elif method == "stop":
        constants.__stopTime__ = datetime.now()
        setStatus("Calculating time", verbose=verbose)
    elif method == "result":
        constants.__time__ = f"Wall time: {(constants.__stopTime__ - constants.__startTime__).total_seconds()}s"
        return constants.__time__


def parseDbs(db, base="mysql", default=True):
    if not db:
        if default:
            db = constants.__dbMysql__ if base == "mysql" else constants.__dbSqlite__
    return Exec([], db)._Exec__parseDatabase()


def parseTables(tables, db):
    return Exec(tables, db)._Exec__parseCommands()


# &SQLite
def __tbToCsv(data, tblName, db="", tbl=True, database=True, index=False):
    timer("start")
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
    timer("stop")
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
    def __init__(
        self,
        command,
        db=[],
        # matrix=True,
        # inlineData=False,
        # splitByColumns=False,
        # asyncExec=False,
        # splitExec=False,
        returnDict=False,
        verbose=False,
        err=True,
        simplify=False,
        # commit=True,
    ):
        self.__command = command  # Commands to be executed
        self.db = db  # Database
        # self.__matrix = matrix
        # self.__inlineData = inlineData
        # self.__splitByColumns = splitByColumns
        # self.__asyncExec = asyncExec
        self.__returnDict = returnDict
        self.__verbose = verbose  # For being verbose
        self.__raiseError = err  # Raise error or not if something goes wrong
        self.__simplify = simplify  # Commit the changes
        # self.__commit = commit
        self.__result = []
        self._driver = None
        self.base = "mysql"
        self.property = ()

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
        return tools.timer("result")

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

    def execute(self):
        self.__simplify = constants.simplify if not self.__simplify else self.__simplify
        constants.__pid__ = os.getpid()
        tools.timer("start", self.__verbose)

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
            tools.setStatus("Connected", verbose=self.__verbose)
            tools.setStatus("Creating the pointer", verbose=self.__verbose)

            data = []
            tools.setStatus(
                f"Executing command database: {self.db[i]}", verbose=self.__verbose
            )
            for comm in self.__command[i]:
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

        tools.setStatus("Preparing results", verbose=self.__verbose)
        tools.timer("stop", self.__verbose)

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
        tools.setStatus("Parsing commands", self.__raiseError, self.__verbose)
        try:
            db = tools.parseDbs(db) if db else tools.parseDbs(self.db)
            if len(db) == 1:
                if tools.checkInstance(command, str):
                    command = [[command]]
                elif tools.checkInstance(command[0], str):
                    command = [command]
                elif tools.checkInstance(command[0], list, tuple):
                    if tools.checkInstance(command[0][0], list, tuple):
                        raise exception.UnknownError(
                            "Database and commands are not commuting, n(commands) != n(database)"
                        )
                else:
                    raise exception.CommandError()
            else:
                if tools.checkInstance(command, list, tuple):
                    if not tools.checkInstance(
                        command[0], list, tuple
                    ) or tools.checkInstance(command[0][0], list, tuple):
                        raise exception.UnknownError(
                            "Database and commands are not commuting, n(commands) != n(database)"
                        )
                else:
                    raise exception.CommandError()
        except IndexError:
            if self.__raiseError:
                raise exception.CommandError("Command not provided")
        except Exception as e:
            if self.__raiseError:
                raise e
        return command

    def __parseDatabase(self, db=""):
        db = db if db else self.db
        tools.setStatus("Parsing database(s)", self.__raiseError, self.__verbose)
        try:
            if tools.checkInstance(db, str):
                db = [db]
            elif not tools.checkInstance(db, list, tuple) or not tools.checkInstance(
                db[0], str
            ):
                raise ValueError
        except Exception:
            pass

        if not db:
            db = constants.__dbSqlite__ if self.base == "sqlite" else constants.__dbMysql__
        if not db:
            if self.__raiseError:
                raise exception.DatabaseError()
            else:
                tools.setStatus("Error while parsing databases", verbose=True)
        return db
