import os.path
from cubes.server import create_server

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Set the configuration file name (and possibly whole path) here
CONFIG_PATH = os.path.join(CURRENT_DIR, "slicer.ini")

application = create_server(CONFIG_PATH)
