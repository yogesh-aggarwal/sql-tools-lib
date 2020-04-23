import time

from flask import Flask, render_template

from sql_tools import constants

app = Flask(
    __name__,
    static_url_path="/static",
    static_folder="./web/static",
    template_folder="./web/templates",
)

import logging

logging.getLogger("werkzeug").disabled = True


@app.route("/api")
def api():
    data = {
        "__dbSqlite__": constants.__dbSqlite__,
        "__dbMysql__": constants.__dbMysql__,
        "__time__": constants.__time__,
        "__startTime__": constants.__startTime__,
        "__stopTime__": constants.__stopTime__,
        "__history__": constants.__history__,
        "__pid__": constants.__pid__,
        "__error__": False,  # TODO: Pending to implement in the main library
        "time": time.time(),
    }
    return str(data)


@app.route("/")
def explore():
    return render_template("explore.html")
