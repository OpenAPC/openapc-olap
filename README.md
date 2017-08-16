openapc-olap is a small [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) Server based on [cubes](http://cubes.databrewery.org/). It offers a fast and efficient way of querying the [OpenAPC data](https://github.com/OpenAPC/openapc-de) and also works as backend for the OpenAPC treemaps server.

Installation (requires a working PostgreSQL installation):

    clone or download/unzip
    cd openapc-olap
    virtualenv venv (Create a python virtual environment)
    pip install -r requirements.txt
    python assets_generator.py db_settings (Generates a credentials file for the database)
    sudo -u postgres psql -f setup.sql -v pw="'secret'" (Set up a database with roles and schema. Change the 'pw' parameter to something more sophisticated and copy the value to the 'pass' field in db_settings.ini, without any quotes.)
    python assets_generator.py model (Generates a model file for the cubes server.)
    python assets_generator.py tables (Create and populate the database tables. Requires the openapc core data file (apc_de.csv) and the offsetting file (offsetting.csv) to be present in the directory.)
    python olap_server.py

These instructions will fire up a [flask](http://flask.pocoo.org/)-based development server at localhost under port 3001 (Can be modified in cubes_server.py). For a long-term setup you should deploy a [WSGI-based configuration](https://pythonhosted.org/cubes/deployment.html).
