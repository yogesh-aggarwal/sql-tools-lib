from . import execute, constants, mysqlException, __tools


#  TODO: Move database
#  TODO: Copy database
#  TODO: Is identical

def clearDb(db, raiseError=True):
    try:
        db = __tools.parse(db)
        [execute([f"DROP DATABASE {x};"], "mysql", _execute__execMethod=False).execute() for x in db]
        [execute([f"CREATE DATABASE {x};"], "mysql", _execute__execMethod=False).execute() for x in db]
    except Exception as e:
        if raiseError:
            raise e
    return True


def createDb(db, raiseError=True):
    db = __tools.parse(db)
    for x in db:
        try:
            execute([f"CREATE DATABASE {x};"], ["mysql"], _execute__execMethod=False).execute()
        except Exception as e:
            if raiseError:
                raise e
    return True


def delDb(db, raiseError=True):
    db = __tools.parse(db)
    for x in db:
        try:
            execute([f"DROP DATABASE {x};"], ["mysql"], _execute__execMethod=False).execute()
        except Exception as e:
            if raiseError:
                raise e
    return True

def isIdentical():
    pass
