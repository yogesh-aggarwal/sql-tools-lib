from flask import Flask
from sql_tools import constants


app = Flask(__name__)


@app.route("/")
def index():
    return str(constants.__dbSqlite__)
