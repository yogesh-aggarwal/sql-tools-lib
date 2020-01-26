import warnings

from . import execute, tools

#  TODO: Move database
#  TODO: Copy database

warnings.simplefilter(action="ignore", category=FutureWarning)


def clearDb(db, raiseError=True):
    try:
        db = tools.parseDbs(db)
        delDb(db)
        createDb(db)
    except Exception as e:
        if raiseError:
            raise e
    return True


def createDb(db, raiseError=True):
    db = tools.parseDbs(db)
    for x in db:
        try:
            execute(
                [f"CREATE DATABASE {x};"], ["mysql"], _execute__execMethod=False
            ).execute()
        except Exception as e:
            if raiseError:
                raise e
    return True


def delDb(db, raiseError=True):
    db = tools.parseDbs(db)
    for x in db:
        try:
            execute(
                [f"DROP DATABASE {x};"], ["mysql"], _execute__execMethod=False
            ).execute()
        except Exception as e:
            if raiseError:
                raise e
    return True


def showDbs(raiseError=True):
    return [x[0] for x in execute(["SHOW DATABASES;"], ["mysql"]).list[0][0]]


def isIdentical(db, raiseError=True):
    db = tools.parseDbs(db)
    tables = [execute(["SHOW TABLES"], x).list for x in db]
    tables = list(filter(lambda x: x != tables[0], tables))
    if len(tables) != 0:
        print(False)
        return False
    else:
        tables = [execute(["SHOW TABLES"], x).list[0][0] for x in db]
        tables = [[y[0] for y in x] for x in tables]

        records = []
        for i, x in enumerate(tables):
            for table in x:
                records.append(execute([f"SELECT * FROM {table}"], db[i]).list)
        records = list(filter(lambda x: x != records[0], records))

        if len(records) != 0:
            del tables, records
            return False
        else:
            del tables, records
            return True
