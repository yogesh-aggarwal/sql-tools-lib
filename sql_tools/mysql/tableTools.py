import numpy as np
import pandas as pd
from prettytable import PrettyTable

import sql_tools.internals as tools
from sql_tools import constants, exception

from . import execute


def getTbls(db="", err=True, verbose=False):
    db = tools.parseDbs(db)
    return [
        [x[0] for x in execute(["SHOW TABLES"], db=datab, err=err, verbose=verbose).list[0][0]] for datab in db
    ]


def csvToTbl(
    db, table, csv, primaryKey="", foreignKey="", err=True, verbose=False
):
    tools.setStatus("Parsing parameters", verbose=verbose, err=err)
    dbs = tools.parseDbs(db)
    tables = tools.parseTables(table, db)
    csvs = tools.parseTables(csv, db)

    for i, db in enumerate(dbs):
        for k, table in enumerate(tables[i]):
            tools.setStatus(f"Reading CSV: {csvs[i][k]}", verbose=verbose, err=err)
            data = pd.read_csv(csvs[i][k])

            if primaryKey == foreignKey:
                if err:
                    exception.KeyError("Primary & Foreign keys are same.")
                else:
                    tools.setStatus("Primary & Foreign keys are same.", verbose=True)
                    exit()

            attributes = ""
            for column in data.columns.values:
                # &Generating the query
                tools.setStatus("Generating temporary query for table", verbose=verbose, err=err)
                attributes += f"{column} TEXT, "

            attributes = attributes[:-2]

            try:
                tools.setStatus(f"Creating table: {table}", verbose=verbose, err=err)
                execute(f"CREATE TABLE {table} ({attributes});", db=db)
            except Exception as e:
                if err:
                    raise e
                else:
                    tools.setStatus(e, verbose=True, err=err)
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

            tools.setStatus("Inserting records", verbose=verbose, err=err)
            for record in final:
                record = record.replace("nan", "NULL")
                execute([f"INSERT INTO {table} VALUES ({record})"], verbose=verbose, db=db)

            tools.setStatus("Manipulating table for end use", verbose=verbose, err=err)
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
                    # &Generating the query
                    execute(
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dtypeColumn}'] if raw else dtypeColumn} PRIMARY KEY",
                        db=db,
                    )
                elif column.lower() == foreignKey.lower():
                    execute(
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']} FOREIGN KEY",
                        db=db,
                    )
                else:
                    execute(
                        f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']}",
                        db=db,
                    )


def execFile(file, db="", out=True, err=True, verbose=False):
    tools.setStatus("Pasing objects", verbose=verbose, err=err)
    datab = tools.parseDbs(db)
    files = tools.parseTables(file, datab)

    for i, db in enumerate(datab):
        for file in files[i]:
            try:
                tools.setStatus("Reading file", verbose=verbose, err=err)
                file = open(file)
                sql = file.read().replace("\n", "").split(";")
                try:
                    sql.remove("")
                except:
                    pass
                file.close()
                tools.setStatus(f"Executing file", verbose=verbose, err=err)
                result = execute([sql], db, verbose=verbose).get
                if out:
                    tools.setStatus("Printing results", verbose=verbose, err=err)
                    print(result)
            except Exception as e:
                if err:
                    raise exception.UnknownError(e)
                else:
                    tools.setStatus(e, verbose=True, err=err)
                return False
    return True


def getCNames(tbl, db="", err=True, verbose=False):
    dbs = tools.parseDbs(db)
    return [
        [
            execute([f"SHOW columns FROM {y}"], db=dbs[x], verbose=verbose).get.T[0].T[0][0].tolist()
            for y in tools.parseTables(tbl, dbs)[x]
        ]
        for x in range(len(dbs))
    ]


def showTbl(tbl, db="", sep=("%", 30), err=True, verbose=False):
    tools.setStatus("Parsing parameters", verbose=verbose, err=err)
    dbs = tools.parseDbs(db)
    sep = tools.parseDbs(sep)
    tbls = tools.parseTables(tbl, dbs)
    for i, db in enumerate(dbs):
        tools.setStatus(f"For database: {db}", verbose=verbose, err=err)
        for tbl in tbls[i]:
            data = None
            try:
                tools.setStatus(f"Retrieving records for table: {tbl}", verbose=verbose, err=err)
                data = execute([f"SELECT * FROM {tbl}"], db).get[0][0]
            except Exception as e:
                if err:
                    raise e

            tools.setStatus("Preparing table", verbose=verbose, err=err)
            _tbl = tbl
            print(f"{sep[0]*sep[1]} [Table: {_tbl}] {sep[0]*sep[1]}")
            tbl = PrettyTable()
            tbl.field_names = getCNames(_tbl, db)[0][0]
            del _tbl
            [tbl.add_row(x) for x in data]
            tools.setStatus("Printing table", verbose=verbose, err=err)
            print(f"{tbl}\n\n")
