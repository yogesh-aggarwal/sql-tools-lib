import sql_tools.internals as tools
from sql_tools import constants, exception

from . import driver


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
    dbs = tools.parseDbs(db)
    for db in dbs:
        try:
            driver.connect(
                host=host,
                user=user,
                password=pword,
                db=db if db else "mysql",
                charset=charset,
            ).close()
        except Exception:
            if err:
                raise exception.ConnectionError(
                    "Cannot connect to MySQL server, it may be server or credential problem"
                )
        tools.setStatus(
            f"Credentials are correct, connecting to database: {db}",
            verbose=verbose,
            err=err,
        )
    constants.__credentials__ = (host, user, pword)
    constants.__charset__ = charset

    tools.setStatus(
        "Manipulating database for SQL-Tools execution",
        verbose=verbose,
        err=err,
    )
    if tools.checkInstance(dbs, list, tuple):
        constants.__dbMysql__.extend(dbs)
    else:
        if err:
            raise exception.DatabaseError(
                "Invalid database dtype, it should be str or array"
            )  
        else:
            tools.setStatus(
                "Invalid database dtype, it should be str or array",
                verbose=True,
                err=err,
            )


def disconnect(db=[], err=True, verbose=False):
    db = tools.parseDbs(db)
    tools.setStatus("Disconnecting database", verbose=verbose, err=err)
    constants.__dbMysql__ = [x for x in constants.__dbMysql__ if x not in db]
