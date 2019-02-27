import os.path

activate_this = '/var/www/wsgi-scripts/openapc-olap/venv/bin/activate_this.py'
exec(open(activate_this).read(), {'__file__': activate_this})

from cubes.server import create_server
from cubes.config_parser import read_slicer_config
from flask_cors import CORS

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Set the configuration file name (and possibly whole path) here
CONFIG_PATH = os.path.join(CURRENT_DIR, "slicer_wsgi.ini")
CONFIG = read_slicer_config(CONFIG_PATH)

application = create_server(CONFIG)
CORS(application)
