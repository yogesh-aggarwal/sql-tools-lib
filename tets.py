import sql_tools

datab = sql_tools.Sqlite3(databPath="test-db.db")
datab.execute("Cr")
