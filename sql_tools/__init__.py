"""
An integrative library that contains tools for performing various tasks related to the table relations.
"""
from .sqlite import constants


__version__ = "SQL-Tools version: 0.2.5"

if __name__ == "__main__":
    from pathlib import Path
    print("Welcome to the SQL Tools package.")
    with open(f"{str(Path.home())}/AppData/Local/Programs/Python/Python37/lib/site-packages/sql_tools/HELP", "r") as f:
        print(f.read())
        input()
