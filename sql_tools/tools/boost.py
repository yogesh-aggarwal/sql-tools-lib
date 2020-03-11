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


def startInterface(method="sqlite", asyncExec=True):
    def run():
        try:
            import django
            import webbrowser

            webbrowser.open_new_tab("http://127.0.0.1:3400")
            os.chdir(f"{os.getcwd()}\\sql_tools\\interface")
            os.system("python manage.py runserver 3400")
        except ModuleNotFoundError:
            from colorama import init as ansi
            ansi()

            print(
                f'\033[1;31;40;_You must install django>3.0.0 to use the SQL-Tools web interface. Run "pip install django --user" to install it.\033[0m\n'
            )
            exit(0)

    Thread(target=run).start() if asyncExec else run()
