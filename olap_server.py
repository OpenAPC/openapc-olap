from flask import Flask
from sqlalchemy import create_engine
from cubes.server import slicer
from flask.ext.cors import CORS

app = Flask(__name__)
CORS(app)
app.register_blueprint(slicer, config="slicer.ini")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3001)
