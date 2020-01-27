"""
SQL-Tools extension for miscelleneous tools
"""

import time

import requests

from sql_tools import exception


def fetchSQL(url, file="", err=True, verbose=False):
    if not url:
        if err:
            raise exception.ParameterError("Invalid URL provided")
        return False
    else:
        file = file if file else f"{str(time.time())}.sql"
        
        with open(file, "w+") as f:
            f.write(requests.get(url).text)
    return True
