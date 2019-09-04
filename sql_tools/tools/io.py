"""
Contains methods related to connection(s) between database and file.
"""

from .data import *
from .__tools import *
from sql_tools import sqlite
import json

sample = dict(
    {
        "database1": {
            "table1_d1": {
                "name_d1": ["record1", "record2"],
                "age_d1": [16, 20],
                "hobby_d1": ["hacking", "programming"],
            },
            "table2_d1": {
                "field_d1": ["record1", "record2"],
                "passion_d1": ["record1", "record2"],
                "pass_d1": ["hacking", "programming"],
            },
        },
        "database2": {
            "table1_d2": {
                "name_d2": ["record1", "record2"],
                "age_d2": [16, 20],
                "hobby_d2": ["hacking", "programming"],
            },
            "table2_d2": {
                "field_d2": ["record1", "record2"],
                "passion_d2": ["record1", "record2"],
                "pass_d2": ["hacking", "programming"],
            },
        },
    }
)


def jsonToSqlite(jsonPath, databPath, warning=True):
    data = sample
    allDatabases = [x for x in data]
    allTables = [list(data[x]) for x in allDatabases]
    allFields = []
    for x in allDatabases:
        tablesTemp = data[x]
        allFields.append([list(tablesTemp[c]) for c in tablesTemp])
    allDtypes = {}
    for x in allDatabases:
        tables = data[x]
        tabledType = {}
        for i in tables:
            columns = tables[i]
            columndType = []
            for attribute in columns:
                columndType.append(dataType(data[x][i][attribute]))
            tabledType[i] = columndType
        allDtypes[x] = tabledType

    print(f"Databases: {allDatabases}\n\n")
    print(f"Tables: {allTables}\n\n")
    print(f"Fields: {allFields}\n\n")
    print(f"Dtypes: {allDtypes}\n\n")

    for x in allDatabases:
        sqlite.createDatabase(f"test/tools/{x}.sqlite3")
        sqlite.connect(f"test/tools/{x}.sqlite3")

        # Tables
        for y in data[x]:
            columnData = []
            for ind, z in enumerate(allDtypes[x][y]):
                try:
                    columnData.append(f"{list(data[x][y].keys())[ind]} {z}")
                except Exception:
                    pass
            columnData = ", ".join(columnData)
            try:
                sqlite.execute(f"CREATE TABLE {y}({columnData})")
            except Exception:
                setStatus(f"Table ({y}) already exists [from {x}]", logConsole=warning)
                sqlite.execute(f"CREATE TABLE IF NOT EXISTS {y}({columnData})")

            break

        sqlite.Database().clear()

    return True
