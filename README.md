openapc_cubes is a minimal [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) Server based on [cubes](http://cubes.databrewery.org/), meant to provide a backend for [OpenAPC data](https://github.com/OpenAPC).

Installation:
    clone or download/unzip
    cd openapc_cubes
    virtualenv venv (Create a python virtual environment)
    pip install -r requirements.txt
    python init_db.py (At the moment this will try to create a SQLite database at /var/local/openapc_cubes. If you want to use another database/path/credentials you have to change the SQLAlchemy URI in init_db.py and slicer.ini)
    python cubes_server.py
