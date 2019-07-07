import time

from . import __tools, constants, sqliteException
from .execute import execute
from .fetch import *


def swapColumns(tableName, oldCName, newCname, databPath="", returnDict=False):  # PENDING
    """
    Swaps the columns provided.
    """
    if isinstance(tableName, list):
        raise sqliteException.SupportError("Multiple table names aren't supported for this method at the moment.")
    if isinstance(databPath, list):
        raise sqliteException.SupportError("Multiple databases aren't supported for this method at the moment.")
    constants.__startTime__ = time.time()
    if not databPath:
        databPath = constants.__databPath__
        if isinstance(databPath, str):
            databPath = []
            databPath.append(constants.__databPath__)
        elif isinstance(databPath, list) or isinstance(databPath, tuple):
            databPath = []
            databPath.extend(constants.__databPath__)
        if databPath == []:
            raise sqliteException.PathError("Please provide a valid database path.")
    else:
        __temp_lst__ = []
        __temp_lst__.append(databPath)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        databPath = __temp_lst__.copy()
        del __temp_lst__

    __temp_lst__ = []
    __temp_lst__.append(tableName)
    if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
        __temp_lst__ = __temp_lst__[0]
    elif isinstance(__temp_lst__[0], str):
        pass
    else:
        raise sqliteException.PathError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
    tableName = __temp_lst__.copy()
    del __temp_lst__

    if len(tableName) != len(databPath):
        raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a matrix.")

    final = []
    for i in range(len(databPath)):
        try:
            command = getTableCommand(tableName=tableName[i], databPath=databPath[i])[0][0][0].replace("\n", " ").replace("\t"," ").replace("`", "").replace(" (", "(").replace("(", " (").split(" ")
            print(command)
            # quit(0)
        except AttributeError as e:
            print(e)
            raise sqliteException.ColumnError("Error in columns or it may doesn't exists.")  # PENDING
        except Exception as e:
            raise e
        
        oldIndex = [command.index(x) for x in oldCName]

        __tools.setStatus(f"Getting schema for {tableName[i]}")

        for j in range(len(newCname)):
            command[oldIndex[j]] = newCname[j]

        __tools.setStatus(f"Creating a shallow copy of database {databPath[i]}")
        command = " ".join(command).replace(tableName[i], f"temp_sql_tools_159753_token_copy_{tableName[i]}")

        done = []
        print(newCname)
        print(oldCName)
        try:
            __tools.setStatus("Preparing databases for operation")
            execute(command, databPath=databPath[i], _Sqlite3__execMethod=False)
            execute(f"INSERT INTO temp_sql_tools_159753_token_copy_{tableName[i]} SELECT * FROM {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
            for j in range(len(oldCName)):
                print(newCname[j])
                if oldCName[j] not in done and newCname[j] not in done:
                    execute(f"UPDATE temp_sql_tools_159753_token_copy_{tableName[i]} SET {oldCName[j]} = {newCname[j]} , {newCname[j]} = {oldCName[j]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                    done.append(oldCName[i])
            execute(f"DROP TABLE {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
            execute(f"ALTER TABLE temp_sql_tools_159753_token_copy_{tableName[i]} RENAME TO {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)

            del oldCName, oldIndex, newCname
            final.append(True)
        except Exception as e:
            try:
                execute(f"DROP TABLE temp_sql_tools_159753_token_copy_{tableName[i]}", databPath=databPath[i])
            except:
                pass
            raise e
    return final


if __name__ == "__main__":
    print("advTools extention for SQL-Tools library.")
