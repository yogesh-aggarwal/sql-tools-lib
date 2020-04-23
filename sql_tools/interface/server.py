from .api import app

def serve():
    app.run(debug=True)
