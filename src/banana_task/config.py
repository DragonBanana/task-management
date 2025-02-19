# src/mytaskmanager/config.py
import os
import json

DEFAULT_CONFIG = {
    "db_url": "sqlite:///my_tasks.db",
    "output_dir": "./results",
    "use_cache": True,
    "skip_if_in_progress": False
}

def get_config_file() -> str:
    """
    Returns the path to the user's config file, e.g. ~/.mytaskmanager/config.json
    """
    home = os.path.expanduser("~")
    config_dir = os.path.join(home, ".banana_task")
    os.makedirs(config_dir, exist_ok=True)
    return os.path.join(config_dir, "config.json")

def load_config() -> dict:
    """
    Loads config from disk if present, otherwise returns DEFAULT_CONFIG.
    """
    config_path = get_config_file()
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            user_config = json.load(f)
        # Merge user_config onto the defaults
        merged = {**DEFAULT_CONFIG, **user_config}
        return merged
    else:
        return dict(DEFAULT_CONFIG)

def save_config(new_config: dict) -> None:
    """
    Saves the given config dictionary to disk, merging with the defaults.
    """
    config = load_config()
    config.update(new_config)  # update existing or default
    config_path = get_config_file()
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
