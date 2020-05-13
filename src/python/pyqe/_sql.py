"""Utilities for configuring a connection to a SQL database."""

import os
from os.path import expanduser
import json

CONFIG_DIR = os.path.join(expanduser('~'), '.pyqe')
CONFIG_FILE = 'config.json'
CONFIG_PATH = os.path.join(CONFIG_DIR, CONFIG_FILE)

home = expanduser("~")

def set_configuration(spec):
    """Set the database connection configuration.

    Args:
        spec (dict): A JSON-serializable dictionary with connection info.
    """

    if not os.path.exists(CONFIG_DIR):
       os.makedirs(CONFIG_DIR)
    with open(CONFIG_PATH, 'w') as f:
        f.write(json.dumps(spec))

def get_configuration():
    """Get the database connection configuration.

    Returns:
        dict: The connection configuration.
    """

    if os.path.isfile(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    else:
        return None

def get_db_conn_str():
    """Get a SQL connection string. Note that currently the database is assumed
    to be Postgres."""

    config = get_configuration()
    return f'postgres://{config["user"]}:{config["password"]}@{config["url"]}:{config["port"]}/{config["database"]}'
