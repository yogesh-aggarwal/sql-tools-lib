import numpy as np
import pandas as pd
from prettytable import PrettyTable

import sql_tools.internals as tools

from . import constants, execute, mysqlException
from . import tools as mysqlTools


def getTbls(db="", raiseError=True, verbose=False):
    db = mysqlTools.parseDbs(db)
    return [
        [x[0] for x in execute(["SHOW TABLES"], db=datab, raiseError=raiseError, verbose=verbose).list[0][0]] for datab in db
    ]


def csvToTbl(
    db, table, csv, primaryKey="", foreignKey="", raiseError=True, verbose=False
):
    tools.setStatus("Parsing parameters", verbose=verbose, raiseError=raiseError)
    dbs = mysqlTools.parseDbs(db)
    tables = tools.parseTables(table, db)
    csvs = tools.parseTables(csv, db)

    for i, db in enumerate(dbs):
        for k, table in enumerate(tables[i]):
            tools.setStatus(f"Reading CSV: {csvs[i][k]}", verbose=verbose, raiseError=raiseError)
            data = pd.read_csv(csvs[i][k])

            if primaryKey == foreignKey:
                if raiseError:
                    mysqlException.KeyError("Primary & Foreign keys are same.")
                else:
                    tools.setStatus("Primary & Foreign keys are same.", verbose=True)
                    exit()

            attributes = ""
            for column in data.columns.values:
                # &Generating the query
                tools.setStatus("Generating temporary query for table", verbose=verbose, raiseError=raiseError)
                attributes += f"{column} TEXT, "

            attributes = attributes[:-2]

            try:
                tools.setStatus(f"Creating table: {table}", verbose=verbose, raiseError=raiseError)
                execute(f"CREATE TABLE {table} ({attributes});", db=db)
            except Exception as e:
                raise e if raiseError else tools.setStatus(e, verbose=True, raiseError=raiseError)
            del attributes

            # Inejecting the records
            records = data.values.tolist()
            final = []
            for record in records:
                st = ""
                for column in record:
                    column = str(column).replace('"', '"')
                    st += f'"{column}", '
                final.append(st[:-2])

            tools.setStatus("Inserting records", verbose=verbose, raiseError=raiseError)
            for record in final:
                record = record.replace("nan", "NULL")
                print(f"INSERT INTO {table} VALUES ({record})")
                execute([f"INSERT INTO {table} VALUES ({record})"], verbose=verbose, db=db)

            tools.setStatus("Manipulating table for end use", verbose=verbose, raiseError=raiseError)
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
                    print(
                        1,
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dtypeColumn}'] if raw else dtypeColumn} PRIMARY KEY",
                    )
                    # &Generating the query
                    execute(
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dtypeColumn}'] if raw else dtypeColumn} PRIMARY KEY",
                        db=db,
                    )
                elif column.lower() == foreignKey.lower():
                    print(
                        2,
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']} FOREIGN KEY",
                    )
                    execute(
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']} FOREIGN KEY",
                        db=db,
                    )
                else:
                    print(
                        3,
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']}",
                    )
                    execute(
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']}",
                        db=db,
                    )


def execFile(file, db="", out=True, raiseError=True, verbose=False):
    tools.setStatus("Pasing objects", verbose=verbose, raiseError=raiseError)
    datab = mysqlTools.parseDbs(db)
    files = tools.parseTables(file, datab)

    for i, db in enumerate(datab):
        for file in files[i]:
            try:
                tools.setStatus("Reading file", verbose=verbose, raiseError=raiseError)
                file = open(file)
                sql = file.read().replace("\n", "").split(";")
                try:
                    sql.remove("")
                except:
                    pass
                file.close()
                tools.setStatus(f"Executing file", verbose=verbose, raiseError=raiseError)
                result = execute([sql], db, verbose=verbose).get
                if out:
                    tools.setStatus("Printing results", verbose=verbose, raiseError=raiseError)
                    print(result)
            except Exception as e:
                raise mysqlException.UnknownError(e) if raiseError else tools.setStatus(e, verbose=True, raiseError=raiseError)
                return False
    return True


def getCNames(tbl, db="", raiseError=True, verbose=False):
    dbs = mysqlTools.parseDbs(db)
    return [
        [
            execute([f"SHOW columns FROM {y}"], db=dbs[x], verbose=verbose).get.T[0].T[0][0].tolist()
            for y in tools.parseTables(tbl, dbs)[x]
        ]
        for x in range(len(dbs))
    ]


def showTbl(tbl, db="", sep=("%", 30), raiseError=True, verbose=False):
    tools.setStatus("Parsing parameters", verbose=verbose, raiseError=raiseError)
    dbs = mysqlTools.parseDbs(db)
    sep = mysqlTools.parseDbs(sep)
    tbls = tools.parseTables(tbl, dbs)
    for i, db in enumerate(dbs):
        tools.setStatus(f"For database: {db}", verbose=verbose, raiseError=raiseError)
        for tbl in tbls[i]:
            data = None
            try:
                tools.setStatus(f"Retrieving records for table: {tbl}", verbose=verbose, raiseError=raiseError)
                data = execute([f"SELECT * FROM {tbl}"], db).get[0][0]
            except Exception as e:
                raise e if raiseError else False

            tools.setStatus("Preparing table", verbose=verbose, raiseError=raiseError)
            _tbl = tbl
            print(f"{sep[0]*sep[1]} [Table: {_tbl}] {sep[0]*sep[1]}")
            tbl = PrettyTable()
            tbl.field_names = getCNames(_tbl, db)[0][0]
            del _tbl
            [tbl.add_row(x) for x in data]
            tools.setStatus("Printing table", verbose=verbose, raiseError=raiseError)
            print(f"{tbl}\n\n")
