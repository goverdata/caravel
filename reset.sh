#!/bin/sh

set -x
set -e

rm /tmp/caravel.db

    # [optional] setup a virtual env and activate it
#    virtualenv env
#    source env/bin/activate

    # install for development
#    python setup.py develop

#    fabmanager create-admin --app caravel
    fabmanager create-admin --app caravel --username admin --firstname admin --lastname user --email admin@fab.org --password admin

    # Initialize the database
    caravel db upgrade

    # Create default roles and permissions
    caravel init

    # Load some data to play with
    caravel load_elasticsearch_examples # FIXME ajout MP
    caravel load_examples

    # start a dev web server
    caravel runserver -d

