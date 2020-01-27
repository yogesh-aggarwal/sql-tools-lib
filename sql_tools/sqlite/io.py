# """
# Contains methods related to connection(s) between database and file.
# """

import numpy as np
import pandas as pd

import sql_tools.internals as tools
from sql_tools import constants, exception

from . import execute


# def tbToCsv(tb, db="", returnDict=False, index=False):
#     """
#     Converts table records to a CSV file.
#     """
#     constants.__startTime__ = time.time()
#     db = tools.parseDbs(db)
#     tb = tools.parseTables(tb)

#     if len(tb) != len(db):
#         raise ValueError(
#             "Cannot apply command to the provided data set. Please provide equal table names and paths. Should form a matrix."
#         )

#     final = []
#     for i in range(len(db)):
#         tools.setStatus(f"Converting database to dataframe ({db[i]})")
#         try:
#             final.append(
#                 tools.__tbToCsv(
#                     data=execute(f"SELECT * FROM {tb[i]}")[0],
#                     tb=tb[i],
#                     db=db[i],
#                     index=index,
#                 )
#             )
#         except Exception as e:
#             raise e

#     if returnDict:
#         tools.setStatus("Convering to dictionary")
#         final = dict(zip(tb, final))

#     tools.setStatus("Returning results")
#     constants.__stopTime__ = time.time()
#     constants.__time__ = (
#         f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
#     )
#     return final


# def csvToTbl(
#     db, table, csv, primaryKey="", foreignKey="", err=True, verbose=False
# ):
#     tools.setStatus("Parsing parameters", verbose=verbose, err=err)
#     dbs = tools.parseDbs(db)
#     tables = tools.parseTables(table, db)
#     csvs = tools.parseTables(csv, db)

#     for i, db in enumerate(dbs):
#         for k, table in enumerate(tables[i]):
#             tools.setStatus(f"Reading CSV: {csvs[i][k]}", verbose=verbose, err=err)
#             data = pd.read_csv(csvs[i][k])

#             if primaryKey == foreignKey:
#                 if err:
#                     exception.KeyError("Primary & Foreign keys are same.")
#                 else:
#                     tools.setStatus("Primary & Foreign keys are same.", verbose=True)
#                     exit()

#             attributes = ""
#             for column in data.columns.values:
#                 # &Generating the query
#                 tools.setStatus("Generating temporary query for table", verbose=verbose, err=err)
#                 attributes += f"{column} TEXT, "

#             attributes = attributes[:-2]

#             try:
#                 tools.setStatus(f"Creating table: {table}", verbose=verbose, err=err)
#                 execute(f"CREATE TABLE {table} ({attributes});", db=db)
#             except Exception as e:
#                 if err:
#                     raise e
#                 else:
#                     tools.setStatus(e, verbose=True, err=err)
#             del attributes

#             # Inejecting the records
#             records = data.values.tolist()
#             final = []
#             for record in records:
#                 st = ""
#                 for column in record:
#                     column = str(column).replace('"', '"')
#                     st += f'"{column}", '
#                 final.append(st[:-2])

#             tools.setStatus("Inserting records", verbose=verbose, err=err)
#             for record in final:
#                 record = record.replace("nan", "NULL")
#                 print(f"INSERT INTO {table} VALUES ({record})")
#                 execute([f"INSERT INTO {table} VALUES ({record})"], verbose=verbose, db=db)

#             tools.setStatus("Manipulating table for end use", verbose=verbose, err=err)
#             dtypes = constants.dtypeMap
#             dataDtypes = data.dtypes
#             for column in data.columns.values:
#                 if column.lower() == primaryKey.lower():
#                     dtypeColumn = dataDtypes[column]
#                     # &Checking whether the type is object, if yes, then setting it to VARCHAR with length of max length of the value in the column
#                     dtypeColumn = (
#                         f"VARCHAR({np.vectorize(len)(data.select_dtypes(include=[object]).values.astype(str)).max(axis=0)[data.columns.get_loc(primaryKey)]})"
#                         if dtypeColumn == "object"
#                         else dtypeColumn
#                     )
#                     # &Checking whether the dtype is changed to VARCHAR or not
#                     raw = True if dtypeColumn in dtypes.keys() else False
#                     print(
#                         1,
#                         f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dtypeColumn}'] if raw else dtypeColumn} PRIMARY KEY",
#                     )
#                     # &Generating the query
#                     execute(
#                         f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dtypeColumn}'] if raw else dtypeColumn} PRIMARY KEY",
#                         db=db,
#                     )
#                 elif column.lower() == foreignKey.lower():
#                     print(
#                         2,
#                         f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']} FOREIGN KEY",
#                     )
#                     execute(
#                         f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']} FOREIGN KEY",
#                         db=db,
#                     )
#                 else:
#                     print(
#                         3,
#                         f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']}",
#                     )
#                     execute(
#                         f"ALTER TABLE {table} MODIFY COLUMN {column} {dtypes[f'{dataDtypes[column]}']}",
#                         db=db,
#                     )

# def dbToCSV(db="", returnDict=False):
#     """
#     Converts the data infoformation to a CSV file.
#     """
#     constants.__startTime__ = time.time()
#     db = tools.parseDbs(db)

#     final = []
#     for i in range(len(db)):
#         tools.setStatus(f"Creating CSV of {db[i]}")
#         final.append(
#             tools.__tbToCsv(
#                 data=execute("SELECT * FROM sqlite_master")[0],
#                 tb="",
#                 db=db[i],
#                 tbl=False,
#             )
#         )

#     if returnDict:
#         tools.setStatus("Packing into dictionary")
#         final = dict(zip(db, final))

#     tools.setStatus("Returning results")
#     constants.__stopTime__ = time.time()
#     constants.__time__ = (
#         f"Wall time: {(constants.__stopTime__ - constants.__startTime__)*10}s"
#     )
#     return final


# if __name__ == "__main__":
#     print("File-Database extention for SQL-Tools library.")
