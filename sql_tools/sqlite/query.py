from sql_tools.exception import QueryError


class _Query_Gen:
    """
    For internal uses only, don't use it in your main code
    """
    def __init__(self):
        pass


class Query:
    """
    Base class for executing `raw` queries.
    """
    def __init__(self, query=""):
        self.query = query

    def execute(self, query=""):
        query = query if query else self.query

        if not query:
            raise QueryError(f'Expected a SQL query but got "{query}"')
