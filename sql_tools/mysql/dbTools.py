import warnings

import sql_tools.internals as tools

from . import execute
from . import tools as mysqlTools

#  TODO: Move database
#  TODO: Copy database


warnings.simplefilter(action="ignore", category=FutureWarning)


def clearDb(db="", raiseError=True):
    try:
        db = mysqlTools.parseDbs(db)
        delDb(db)
        createDb(db)
    except Exception as e:
        raise e if raiseError else False
    return True


def createDb(db, raiseError=True, verbose=False):
    db = mysqlTools.parseDbs(db)
    for x in db:
        try:
            tools.setStatus(f"Creating: {x}", verbose=verbose, raiseError=raiseError)
            execute(
                [f"CREATE DATABASE {x};"],
                ["mysql"],
                verbose=verbose,
                _execute__execMethod=False,
            ).execute()
        except Exception as e:
            raise e if raiseError else False
    return True


def delDb(db="", raiseError=True, verbose=False):
    db = mysqlTools.parseDbs(db)
    for x in db:
        try:
            tools.setStatus(f"Deleting: {x}", verbose=verbose, raiseError=raiseError)
            execute(
                [f"DROP DATABASE {x};"],
                ["mysql"],
                verbose=verbose,
                _execute__execMethod=False,
            ).execute()
        except Exception as e:
            raise e if raiseError else False
    return True


def getDbs(raiseError=True, verbose=False):
    try:
        return [
            x[0]
            for x in execute(["SHOW DATABASES;"], ["mysql"], verbose=verbose).list[0][0]
        ]
    except Exception as e:
        raise e if raiseError else False


def isIdentical(db="", raiseError=True, verbose=False):
    db = mysqlTools.parseDbs(db)
    tables = [execute(["SHOW TABLES"], x).list for x in db]
    try:
        tables = list(filter(lambda x: x != tables[0], tables))
        tools.setStatus(
            "Comparing tables properties", verbose=verbose, raiseError=raiseError
        )
        if len(tables) != 0:
            print(False)
            return False
        else:
            tools.setStatus(
                "Comparing tables records", verbose=verbose, raiseError=raiseError
            )
            tables = [
                execute(["SHOW TABLES"], x, verbose=verbose).list[0][0] for x in db
            ]
            tables = [[y[0] for y in x] for x in tables]

            records = []
            for i, x in enumerate(tables):
                for table in x:
                    records.append(
                        execute([f"SELECT * FROM {table}"], db[i], verbose=verbose).list
                    )
            records = list(filter(lambda x: x != records[0], records))

            if len(records) != 0:
                del tables, records
                return False
            else:
                del tables, records
                return True
    except Exception as e:
        raise e if raiseError else False
