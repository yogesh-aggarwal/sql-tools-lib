from sql_tools import sqlite

def raiseError(error):
    print(f"Traceback (most recent call last):\n{error}")


class Parse:
    def __init__(self, command):
        self.command = command
        self.source = None
        self.sh = []
        self.dh = []
        self.words = []
    
    def parse(self):
        command =self.command.split(" ")
        self.source = command[0]
        for index, word in enumerate(command):
            dh = None
            sh = None
            try:
                if word[:2] == "--":
                    self.dh.append((index, word[2:]))
                    dh = word
            except:
                pass
            try:
                if word[0] == "-" and word[1] != "-":
                    self.sh.append((index, word[1:]))
                    sh = word
            except:
                pass
            try:
                if word != self.source and word != sh and word != dh:
                    self.words.append(word)
            except:
                pass
        def printData():
            print("\n\n{{ Data start }}")
            print(f"| | |\tSingle hyphen: {self.sh}")
            print(f"| | |\tDouble hyphen: {self.dh}")
            print(f"| | |\tWords: {self.words}")
            print(f"| | |\tSource: [{self.source}]")
            print("{{ Data end }}\n\n")
        
        # printData()

        Analyse(self.command, self.source, self.sh, self.dh, self.words)


class Analyse:
    def __init__(self, command, source, sh, dh, words):
        self.command = command
        self.source = source
        self.sh = sh
        self.dh = dh
        self.words = words

        self.classify()
    
    def classify(self):
        if self.source == "table":
            Table(self.words, self.sh, self.dh)
        elif self.source == "connect":
            Connect(self.words, self.sh, self.dh)
        elif self.source == "disconnect":
            Disconnect(self.words, self.sh, self.dh)
        else:
            if self.source != "":
                raiseError(f'Invalid command "{self.source}"')
    
    @staticmethod
    def tableClassify(words, sh, dh):
        if "all" in words:
            Table.all()

    @staticmethod
    def connectClassify(words, sh, dh):
        for option in sh:
            if option[1] == "f":
                Connect.force(words)
                break

        if sh == []:
            Connect.connect(words)
    
    @staticmethod
    def disconnectClassify(words, sh, dh):
        for option in sh:
            if option[1] == "f":
                Disconnect.force(words)
                break

        if sh == []:
            Disconnect.disconnect(words)


class Table:
    def __init__(self, words, sh, dh):
        self.words = words
        self.sh = sh
        self.dh = dh

        Analyse.tableClassify(words, sh, dh)

    @staticmethod
    def all():
        try:
            tables = sqlite.getTNames()
            databases = sqlite.constants.__databPath__
            print("Tables:")
            for i in range(len(tables)):
                print(f"{databases[i]}: {tables[i]}")
        except Exception as e:
            raiseError(e)


class Connect:
    def __init__(self, words, sh, dh):
        self.words = words
        self.sh = sh
        self.dh = dh

        Analyse.connectClassify(words, sh, dh)

    @staticmethod
    def connect(path):
        try:
            sqlite.connect(path)
        except Exception as e:
            raiseError(e)

    @staticmethod
    def force(path):
        try:
            sqlite.connect(path, validateDatabase=False)
        except Exception as e:
            raiseError(e)


class Disconnect:
    def __init__(self, words, sh, dh):
        self.words = words
        self.sh = sh
        self.dh = dh

        Analyse.disconnectClassify(words, sh, dh)

    @staticmethod
    def disconnect(path):
        try:
            sqlite.disconnect(path)
        except Exception as e:
            raiseError(e)

    @staticmethod
    def force(path):
        sqlite.disconnect(path)


# parseDbs"python3 manage.py runserver --root -f").parseDbs)
# command = "table show -all"
"""
command = "connect -f sample.db users.db"
obj = parseDbscommand)
obj.parseDbs)

command = "table all"
obj = parseDbscommand)
obj.parseDbs)
"""

def start():
    print("Welcome to SQL-Tools interactive shell. Type a valid python syntax to operate.\n")
    try:
        while True:
            command = input(">>> ")
            obj = Parse(command)
            obj.parse()
    except:
        print("\n---> (sql-tools) Quit interactive shell with error code 0 <---")

start()
