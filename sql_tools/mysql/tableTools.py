import numpy as np
import pandas as pd

from . import execute, tools, constants, mysqlException


def getTables(db, raiseError=True, logConsole=False):
    db = tools.parseDbs(db)
    return [[x[0] for x in execute(["SHOW TABLES"], db=datab).list[0][0]] for datab in db]


def csvToTable(
    db, table, csv, primaryKey="", foreignKey="", raiseError=True,
):
    dbs = tools.parseDbs(db)
    tables = tools.parseTables(table, db)
    csvs = tools.parseTables(csv, db)

    for i, db in enumerate(dbs):
        for k, table in enumerate(tables[i]):
            data = pd.read_csv(csvs[i][k])

            if primaryKey == foreignKey:
                if raiseError:
                    mysqlException.KeyError("Primary & Foreign keys are same.")
                else:
                    tools.setStatus("Primary & Foreign keys are same.", logConsole=True)
                    exit()

            attributes = ""
            for column in data.columns.values:
                # &Generating the query
                attributes += f"{column} TEXT, "

            attributes = attributes[:-2]

            try:
                execute(f"CREATE TABLE {table} ({attributes});", db=db)
            except Exception as e:
                raise e
            del attributes

            # Inejecting the records
            records = data.values.tolist()
            final = []
            for record in records:
                st = ""
                for column in record:
                    column = str(column).replace('"', '\"')
                    st += f'"{column}", '
                final.append(st[:-2])

            for record in final:
                record = record.replace("nan", "NULL")
                print(f"INSERT INTO {table} VALUES ({record})")
                execute([f"INSERT INTO {table} VALUES ({record})"], db=db)

            dtypes = constants.dtypeMap
            dataDtypes = data.dtypes
            for column in data.columns.values:
                if column.lower() == primaryKey.lower():
                    dtypeColumn = dataDtypes[column]
                    # &Checking whether the type is object, if yes, then setting it to VARCHAR with length of max length of the value in the column
                    dtypeColumn = (
                        f"VARCHAR({np.vectorize(len)(data.select_dtypes(include=[object]).values.astype(str)).max(axis=0)[data.columns.get_loc(primaryKey)]})"
                        if dtypeColumn == "object"
                        else dtypeColumn
                    )
                    # &Checking whether the dtype is changed to VARCHAR or not
                    raw = True if dtypeColumn in dtypes.keys() else False
                    print(1, f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dtypeColumn}'] if raw else dtypeColumn} PRIMARY KEY")
                    # &Generating the query
                    execute(
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dtypeColumn}'] if raw else dtypeColumn} PRIMARY KEY",
                        db=db,
                    )
                elif column.lower() == foreignKey.lower():
                    print(2, f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']} FOREIGN KEY")
                    execute(
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']} FOREIGN KEY",
                        db=db,
                    )
                else:
                    print(3, f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']}")
                    execute(f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']}", db=db)


def execFile(db, file, out=True, raiseError=True, logConsole=False):
    datab = tools.parseDbs(db)
    files = tools.parseTables(file, datab)

    for i, db in enumerate(datab):
        for file in files[i]:
            try:
                file = open(file)
                sql = file.read().replace("\n", "").split(";")
                try:
                    sql.remove("")
                except:
                    pass
                file.close()
                result = execute([sql], db).get
                if out:
                    print(result)
            except Exception as e:
                if raiseError:
                    raise mysqlException.UnknownError(e)
                return False
    return True
