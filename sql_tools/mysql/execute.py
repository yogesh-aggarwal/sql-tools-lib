"""
Execute extension for SQL-Tools library.
"""

# TODO: Implement the remaining parameters

from . import driver
import os

import numpy as np

from . import tools, constants, mysqlException


class execute:
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
        logConsole=False,
        raiseError=True,
        simplify=False,
        # commit=True,
        __execMethod=True,
    ):
        self.__command = command  # Commands to be executed
        self.db = db  # Database
        # self.__matrix = matrix
        # self.__inlineData = inlineData
        # self.__splitByColumns = splitByColumns
        # self.__asyncExec = asyncExec
        self.__returnDict = returnDict
        self.__logConsole = logConsole  # For being verbose
        self.__raiseError = raiseError  # Raise error or not if something goes wrong
        self.__simplify = simplify  # Commit the changes
        # self.__commit = commit
        self.__execMethod = __execMethod  # Whether called user (True) or internally (False)

        self.__result = []

        self.execute() if self.__execMethod else True

    @property
    def get(self):
        return self.__result

    @property
    def list(self):
        return self.__result.tolist()

    @property
    def dict(self):
        return dict(zip(self.db, self.__result))

    def __toCsv(self, name):
        pass

    def execute(self):
        self.__simplify = constants.simplify if not self.__simplify else self.__simplify
        constants.__processId__ = os.getpid()
        tools.timer("start", self.__execMethod, self.__logConsole)

        # &For database to list
        self.db = self.__parseDatabase()

        # &For command to list
        self.__command = self.__parseCommands()

        # &Unequal condition
        if len(self.__command) != len(self.db):
            raise mysqlException.UnknownError(
                "Database and commands are not commuting, n(commands) != n(database)"
            )

        # &Executing the main command
        # Credentials
        host, user, password, charset = constants.__credentials__

        # Executing commands database wise
        final = []
        for i in range(len(self.db)):
            # Connect to the database
            connection = driver.connect(
                host=host, user=user, password=password, db=self.db[i], charset=charset
            )
            data = []
            for comm in self.__command[i]:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(comm)
                        data.append(list(cursor.fetchall()))
                except Exception as e:
                    if self.__raiseError:
                        raise mysqlException.QueryError(e)
                    return False

            final.append(data)

            connection.commit()
            connection.close()

        tools.setStatus("Preparing results", logConsole=self.__logConsole)
        tools.timer("stop", self.__execMethod, self.__logConsole)

        final = np.array(final)  # For performace
        if self.__simplify:
            if len(self.db) == 1:
                final = final[0]

        self.__result = final

        # &Return dictionary
        if self.__returnDict:
            self.__result = self.dict

    def __parseCommands(self, command="", db=""):
        command = command if command else self.__command
        if self.__execMethod:
            tools.setStatus("Parsing commands", self.__raiseError, self.__logConsole)
        try:
            db = tools.parseDbs(db) if db else tools.parseDbs(self.db)
            if len(db) == 1:
                if tools.checkInstance(command, str):
                    command = [[command]]
                elif tools.checkInstance(command[0], str):
                    command = [command]
                elif tools.checkInstance(command[0], list, tuple):
                    if tools.checkInstance(command[0][0], list, tuple):
                        raise mysqlException.UnknownError("Database and commands are not commuting, n(commands) != n(database)")
                else:
                    raise mysqlException.CommandError()
            else:
                if tools.checkInstance(command, list, tuple):
                    if not tools.checkInstance(command[0], list, tuple) or tools.checkInstance(command[0][0], list, tuple):
                        raise mysqlException.UnknownError("Database and commands are not commuting, n(commands) != n(database)")
                else:
                    raise mysqlException.CommandError()
        except IndexError:
            if self.__raiseError:
                raise mysqlException.CommandError("Command not provided")
        except Exception as e:
            if self.__raiseError:
                raise e
        return command

    def __parseDatabase(self, db=""):
        db = db if db else self.db
        tools.setStatus("Parsing database(s)", self.__raiseError, self.__logConsole)
        try:
            if tools.checkInstance(db, str):
                db = [db]
            elif not tools.checkInstance(db, list, tuple) or not tools.checkInstance(db[0], str):
                raise ValueError
        except Exception:
            pass

        db = constants.__db__ if not db else db
        if not db:
            if self.__raiseError:
                raise mysqlException.DatabaseError()
            else:
                tools.setStatus("Error while parsing databases", logConsole=True)
        return db
