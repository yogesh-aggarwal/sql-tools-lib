import sql_tools.constants as constants

from . import execute

# TODO: Transfer the remaining to sql_tools/internals.py


def parseDbs(db):
    db = constants.__db__ if not db else db
    return execute.execute([], db, _execute__execMethod=False)._execute__parseDatabase()


def parseTables(tables, db):
    return execute.execute(
        tables, db, _execute__execMethod=False
    )._execute__parseCommands()
