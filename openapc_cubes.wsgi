import sys

activate_this = '/var/www/wsgi-scripts/openapc_cubes/venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

sys.path.insert(0, '/var/www/wsgi-scripts/openapc_cubes')

from cubes_server import app as application
