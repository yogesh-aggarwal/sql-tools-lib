"""
SQL-Tools extension for miscelleneous tools
"""

import os
import time
from threading import Thread

import requests

from sql_tools import constants, exception


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


def writeHistoryToFile(file, err=True, verbose=False):
    try:
        with open(file, "w+") as f:
            [
                f.write(
                    x.strip() + "\n" if x.strip()[0] == ";" else f"{x.strip()};" + "\n"
                )
                for x in constants.__history__
            ]
    except Exception as e:
        if err:
            raise e


def parseJson(file):
    data = None
    with open(file) as f:
        data = f.read()
    import json

    data = json.loads(data)
    return {"db": tuple(data.keys()), "command": tuple(data.values())}


def parseFile(file):
    with open(file) as f:
        return f.read().replace("\n", "").split(";")
