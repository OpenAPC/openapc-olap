#!/bin/bash

# Update script for openapc-olap, should be run after changes have been made to OpenAPC data
# This script should be executed from ~/dev

# Pull a fresh copy of the OpenAPC core data file...
cd ~/dev/openapc-de
git pull
cd ~/dev/openapc-olap
# ..and use it to generate the cubes model file and update the DB tables
. venv/bin/activate
python assets_generator.py tables
python assets_generator.py model
deactivate
# Finally, copy the whole directory  
sudo cp -r ~/dev/openapc-olap /var/www/wsgi-scripts

