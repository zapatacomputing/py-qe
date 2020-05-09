import os
from os.path import expanduser
import json

CONFIG_DIR = os.path.join(expanduser('~'), '.pyqe')
CONFIG_FILE = 'config.json'
CONFIG_PATH = os.path.join(CONFIG_DIR, CONFIG_FILE)

home = expanduser("~")
def set_configuration(spec):

    if not os.path.exists(CONFIG_DIR):
       os.makedirs(CONFIG_DIR)
    with open(CONFIG_PATH, 'w') as f:
        f.write(json.dumps(spec))

def get_configuration():
    if os.path.isfile(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    else:
        return None

def get_db_conn_str():
    config = get_configuration()
    return f'postgres://{config["user"]}:{config["password"]}@{config["url"]}:{config["port"]}/{config["database"]}'
