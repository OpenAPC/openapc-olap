openapc_cubes is a minimal [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) Server based on [cubes](http://cubes.databrewery.org/). It offers a fast and efficient way of querying the [OpenAPC data](https://github.com/OpenAPC) and also works as backend for the OpenAPC treemaps server.

Installation:

    clone or download/unzip
    cd openapc_cubes
    virtualenv venv (Create a python virtual environment)
    pip install -r requirements.txt
    python assets_generator.py model (Requires the openapc core data file (apc_de.csv) to be present in the directory. Generates a model file for the cubes server.)
    python assets_generator.py tables (Same prerequisite as above. Generates a SQLite database file. The store url in slicer.ini must point to this file.)
    python cubes_server.py

These instructions will fire up a [flask](http://flask.pocoo.org/)-based development server at localhost under port 3001 (Can be modified in cubes_server.py). For a long-term setup you should deploy a [WSGI-based configuration](https://pythonhosted.org/cubes/deployment.html).
