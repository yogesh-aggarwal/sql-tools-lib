from . import constants, mysqlException, tools


def connect(host, user, pword, db=[], charset="", validate=True, raiseError=True):
    charset = constants.__charset__ if not charset else charset
    constants.__credentials__ = (host, user, pword, charset)

    if tools.checkInstance(db, list, tuple):
        constants.__db__.extend(db)
    elif tools.checkInstance(db, str):
        constants.__db__.append(db)
    else:
        raise mysqlException.DatabaseError("Invalid database dtype, it should be str or array")
