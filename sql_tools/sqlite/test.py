"""
TEST FILE FOR LIBRARY TESTING.
# [Done]
"""

import execute
import connect

connect.connect(["hell.db"])
execute.execute("CREATE TABLE IF NOT EXISTS STUDENT(NAME TEXT);")
