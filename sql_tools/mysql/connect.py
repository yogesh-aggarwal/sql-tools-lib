from . import constants, mysqlException, tools
from . import driver


def connect(
    host,
    user,
    pword,
    db=[],
    charset="",
    validate=True,
    raiseError=True,
    verbose=False,
):
    charset = constants.__charset__ if not charset else charset
    tools.setStatus("Checking credentials", verbose=verbose, raiseError=raiseError)
    try:
        driver.connect(
            host=host,
            user=user,
            password=pword,
            db=db if db else "mysql",
            charset=charset,
        ).close()
    except Exception:
        raise mysqlException.ConnectionError(
            "Cannot connect to MySQL server, it may be server or credential problem"
        )
    tools.setStatus("Credentials are correct, connecting to database", verbose=verbose, raiseError=raiseError)
    constants.__credentials__ = (host, user, pword, charset)

    tools.setStatus("Manipulating database for SQL-Tools execution", verbose=verbose, raiseError=raiseError)
    if tools.checkInstance(db, list, tuple):
        constants.__db__.extend(db)
    elif tools.checkInstance(db, str):
        constants.__db__.append(db)
    else:
        raise mysqlException.DatabaseError(
            "Invalid database dtype, it should be str or array"
        ) if raiseError else tools.setStatus("Invalid database dtype, it should be str or array", verbose=True, raiseError=raiseError)


def disconnect(db=[], raiseError=True, verbose=False):
    db = tools.parseDbs(db)
    tools.setStatus("Disconnecting database", verbose=verbose, raiseError=raiseError)
    constants.__db__ = [x for x in constants.__db__ if x not in db]
