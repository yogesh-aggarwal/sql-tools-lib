"""
A command line tool that contains tools for performing various tasks related to the table relations in extension to SQL-Tools package.
"""

__version__ = "SQL-Tools (CLI) version: 0.0.1"

def start():
    from . import shell
    shell.start()

if __name__ == "__main__":
    print("Welcome to the CLI extension of SQL-Tools package.")
