from .table import Table
from .query import _Query_Gen
from . import driver


class Database:
    def __init__(self, name):
        self.dbName = name
        self.db = None

        self.connect()

    def connect(self):
        self.db = driver.connect(self.dbName)

    def createTable(self, table: Table):
        pass

    def Table(self, name) -> Table:
        return Table()

    # def __str__(self):
    #     pass

    # def __repr__(self):
    #     pass

    # def pickle(self):
    #     pass
