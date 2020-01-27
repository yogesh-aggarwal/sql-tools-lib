"""
SQL-Tools extension for miscelleneous tools
"""

import time

import requests

from sql_tools import exception


def fetchSQL(url, file="", err=True, verbose=False):
    if not url:
        raise exception.ParameterError("Invalid URL provided") if err else False
        return False
    else:
        file = file if file else f"{str(time.time())}.sql"
        
        with open(file, "w+") as f:
            f.write(requests.get(url).text)
    return True
