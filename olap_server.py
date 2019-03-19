from flask import Flask
from configparser import ConfigParser
from cubes.server import slicer
from flask_cors import CORS


app = Flask(__name__)
CORS(app)
config_parser = ConfigParser()
config_parser.read("slicer.ini")
app.register_blueprint(slicer, config=config_parser)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3001)
