"""
An integrative library that contains tools for performing various tasks related to the relations (table records).
"""
from .sqlite import constants
'''
class Sqlite3:
    def swapColumns(self, tableName, oldCName, newCname, databPath="", returnDict=False):
        """
        Swaps the columns provided.
        """
        if isinstance(tableName, list):
            raise ValueError("Multiple table names aren't supported for this method at the moment.")
        if isinstance(databPath, list):
            raise ValueError("Multiple databases aren't supported for this method at the moment.")
        startTime = time.time()
        if not databPath:
            databPath = self.databPath
        else:
            __temp_lst__ = []
            __temp_lst__.append(databPath)
            if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
                __temp_lst__ = __temp_lst__[0]
            elif isinstance(__temp_lst__[0], str):
                pass
            else:
                raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
            databPath = __temp_lst__.copy()
            del __temp_lst__

        __temp_lst__ = []
        __temp_lst__.append(tableName)
        if isinstance(__temp_lst__[0], list) or isinstance(__temp_lst__[0], tuple):
            __temp_lst__ = __temp_lst__[0]
        elif isinstance(__temp_lst__[0], str):
            pass
        else:
            raise ValueError("Invalid path input. Path should be a \"str\" or \"list\" type object.")
        tableName = __temp_lst__.copy()
        del __temp_lst__

        if len(tableName) != len(databPath):
            raise ValueError("Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a square matrix.")

        final = []
        for i in range(len(databPath)):
            command = self.getTableCommand(tableName=tableName[i], databPath=databPath[i])[0][0].replace("\n", " ").replace("\t"," ").replace("`", "").split(" ")
            oldIndex = [command.index(x) for x in oldCName]

            self.__status(f"Getting schema for {tableName[i]}")

            for j in range(len(newCname)):
                command[oldIndex[j]] = newCname[j]

            self.__status(f"Creating a shallow copy of database {databPath[i]}")
            command = " ".join(command).replace(tableName[i], f"temp_sql_tools_159753_token_copy_{tableName[i]}")

            done = []
            print(newCname)
            print(oldCName)
            try:
                self.__status("Preparing databases for operation")
                self.execute(command, databPath=databPath[i], _Sqlite3__execMethod=False)
                self.execute(f"INSERT INTO temp_sql_tools_159753_token_copy_{tableName[i]} SELECT * FROM {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                for j in range(len(oldCName)):
                    print(newCname[j])
                    if oldCName[j] not in done and newCname[j] not in done:
                        self.execute(f"UPDATE temp_sql_tools_159753_token_copy_{tableName[i]} SET {oldCName[j]} = {newCname[j]} , {newCname[j]} = {oldCName[j]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                        done.append(oldCName[i])
                self.execute(f"DROP TABLE {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)
                self.execute(f"ALTER TABLE temp_sql_tools_159753_token_copy_{tableName[i]} RENAME TO {tableName[i]}", databPath=databPath[i], _Sqlite3__execMethod=False)

                del oldCName, oldIndex, newCname
                final.append(True)
            except Exception as e:
                try:
                    self.execute(f"DROP TABLE temp_sql_tools_159753_token_copy_{tableName[i]}", databPath=databPath[i])
                except:
                    pass
                raise e
        return final
'''

__path__ = constants.__databPath__
__time__ = constants.__time__
__status__ = constants.__status__


if __name__ == "__main__":
    from pathlib import Path
    print("Welcome to the SQL Tools package.")
    with open(f"{str(Path.home())}/AppData/Local/Programs/Python/Python37/lib/site-packages/sql_tools/HELP", "r") as f:
        print(f.read())
        input()
