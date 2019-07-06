class QueryError(Exception):
    def __init__(self, message="Error in provided sql query."):
        super().__init__()

class PathError(Exception):
    def __init__(self, message="Error in path provided."):
        super().__init__()

class MatrixError(Exception):
    def __init__(self, message="Error in path provided."):
        super().__init__()

class CommandError(Exception):
    def __init__(self, message="Error in path provided."):
        super().__init__()

class TableError(Exception):
    def __init__(self, message="Error in table name provided."):
        super().__init__()

class DatabaseError(Exception):
    def __init__(self, message="Error in database provided."):
        super().__init__()

class SecurityError(Exception):
    def __init__(self, message="Error in security."):
        super().__init__()

class JSONError(Exception):
    def __init__(self, message="Error in JSON file or it may not in the specified format.\nFormat:\n\t{\n\t\t<database>:\n\t\t{\n\t\t\t<table name>:\n\t\t\t[\n\t\t\t\t<command>\n\t\t\t]\n\t\t}\n\t}"):
        super().__init__()

class UnknownError(Exception):
    def __init__(self, message="Unknown error occured."):
        super().__init__()


if __name__ == "__main__":
    print("Extention of sql-tools package for raising error(s).")
    input()
