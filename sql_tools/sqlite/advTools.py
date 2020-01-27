"""
Advanced tools extension for SQL-Tools library.
"""

from . import tools, sqliteException
from .execute import execute
from .fetch import getCNames, getNRecords, getTNames

# TODO: Swap columns

def validate(databPath="", returnDict=False, raiseError=False, deep=True):
    """
    Vaidates the database whether the database is properly operable or not.
    """
    # For database to list
    tools.setStatus("Validating data")
    try:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise sqliteException.PathError(
                'Invalid path input. Path should be a "str" or "list" type object.'
            )
        databPath = __temp_lst__.copy()
    except Exception:
        raise sqliteException.PathError("Error while parsing your path.")

    final = []
    for database in databPath:
        try:
            tables = getTNames(databPath=database)[0]
            if deep:
                for i in tables:
                    execute(command=f"SELECT * FROM {i}")
                    getCNames(i, databPath=database)
                    getNRecords(i, databPath=database)
            else:
                execute(command=f"SELECT * FROM {tables[0]}")

            final.append(True)
        except Exception:
            if raiseError:
                raise sqliteException.DatabaseError(
                    "The provided database has some problem in it."
                )
            else:
                final.append(False)

    if returnDict:
        final = dict(zip(databPath, final))

    tools.setStatus("Returning data")
    return final


tools = tools


class GenerateChecksum:
    """
    Under development stage, do not use it.
    """

    def __init__(self, databPath="", *args):
        super().__init__()

    def generateSalt(self):
        pass


if __name__ == "__main__":
    print("advTools extention for SQL-Tools library.")
