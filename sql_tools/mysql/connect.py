import sql_tools.internals as tools
from sql_tools import constants

from . import driver, mysqlException


def connect(
    host,
    user,
    pword,
    db=[],
    charset="",
    validateDb=True,
    err=True,
    verbose=False,
):
    charset = constants.__charset__ if not charset else charset
    tools.setStatus("Checking credentials", verbose=verbose, err=err)
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
    tools.setStatus(
        "Credentials are correct, connecting to database",
        verbose=verbose,
        err=err,
    )
    constants.__credentials__ = (host, user, pword, charset)

    tools.setStatus(
        "Manipulating database for SQL-Tools execution",
        verbose=verbose,
        err=err,
    )
    if tools.checkInstance(db, list, tuple):
        constants.__dbMysql__.extend(db)
    elif tools.checkInstance(db, str):
        constants.__dbMysql__.append(db)
    else:
        raise mysqlException.DatabaseError(
            "Invalid database dtype, it should be str or array"
        ) if err else tools.setStatus(
            "Invalid database dtype, it should be str or array",
            verbose=True,
            err=err,
        )


def disconnect(db=[], err=True, verbose=False):
    db = tools.parseDbs(db)
    tools.setStatus("Disconnecting database", verbose=verbose, err=err)
    constants.__dbMysql__ = [x for x in constants.__dbMysql__ if x not in db]
