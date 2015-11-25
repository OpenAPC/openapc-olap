import os.path

activate_this = '/var/www/wsgi-scripts/openapc_cubes/venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

from cubes.server import create_server
from flask.ext.cors import CORS

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Set the configuration file name (and possibly whole path) here
CONFIG_PATH = os.path.join(CURRENT_DIR, "slicer_wsgi.ini")

application = create_server(CONFIG_PATH)
CORS(application)
