import pathlib

import yaml

DEFAULT_DB_URL = ""
DEFAULT_RESULT_DIR = ""

def load_config(path="config.yaml"):

    global DEFAULT_DB_URL, DEFAULT_RESULT_DIR
    """
    Loads configuration from a YAML file.
    Returns a dictionary with all settings.
    """
    with open(path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
        

    user = config['db']["user"]
    password = config['db']["password"]
    host = config['db']["host"]
    port = config['db']["port"]
    name = config['db']["name"]

    DEFAULT_DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{name}"

    output_path = config['app']['output_dir']
    DEFAULT_RESULT_DIR = pathlib.Path(f"results/{output_path}")
    DEFAULT_RESULT_DIR.mkdir(parents=True, exist_ok=True)

    return config

load_config()