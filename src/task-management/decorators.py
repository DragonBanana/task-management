# decorators.py

import functools
import inspect
import json
import os
import time
import pandas as pd
from datetime import datetime

import yaml

from config import DEFAULT_DB_URL, DEFAULT_RESULT_DIR
from task_manager import TaskManager


def task(db_url=DEFAULT_DB_URL, output_dir=DEFAULT_RESULT_DIR):
    """
    Decorator that transforms a function into a "cached" task:

    1. Checks if an identical task (by function name + params) is already COMPLETED.
       - If yes, loads the result from disk (CSV/JSON/TXT) and returns it immediately.
    2. Otherwise, creates a new task (PENDING), sets it to RUNNING, executes the function,
       saves the result to disk, and marks the task COMPLETED.

    Folder structure:
       {output_dir}/{module_name_with_underscores}/{function_name}/

    Special Cases:
    - If the function returns no value (i.e. `None`), we store a small ".txt" file
      with the message "No return value from function".
    - If the function itself writes a file and returns the file path (a string), that string
      is stored in a ".txt" output file. The user can see that path by re-loading the result.

    :param db_url: SQLAlchemy DB URL (e.g., 'postgresql://user:pass@host:port/dbname')
    :param output_dir: Base directory where outputs are saved
    """
    manager = TaskManager(db_url=db_url, echo=False)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 1. Derive a unique task_key from the function name
            func_name = func.__name__
            param_dict = _extract_call_signature(func, args, kwargs)
            task_key = f"{func_name}"

            # Compute folder structure based on module and function name
            module_folder = func.__module__.replace('.', '_')
            save_dir = os.path.join(output_dir, module_folder, func_name)
            os.makedirs(save_dir, exist_ok=True)

            # 2. Check if a COMPLETED task with the same params already exists
            completed_header = manager.find_completed_task(task_key, input_params=param_dict)
            if completed_header is not None:
                return _load_from_disk(
                    completed_header.result_location,
                    completed_header.result_format
                )

            # 3. Otherwise, create a new task in the DB
            random_seed = param_dict.get('random_seed', None)
            new_header = manager.create_task(
                task_key=task_key,
                random_seed=random_seed,
                input_params=param_dict,
                result_location=None,
                result_format=None
            )
            header_id = new_header.id

            # Move to RUNNING
            started = manager.start_task(header_id)
            if not started:
                raise RuntimeError(
                    f"Failed to start task_id={header_id}. Current status is not PENDING."
                )

            # 4. Execute the wrapped function
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                manager.complete_task(header_id, success=False, error_message=str(e))
                raise

            # 5. If the function returned nothing (None), store a small text placeholder
            if result is None:
                result = "No return value from function"

            # Save the result to disk within the save_dir folder
            result_path, result_format = _save_to_disk(
                result,
                base_dir=save_dir,
                task_key=task_key,
                header_id=header_id
            )

            # 6. Mark task COMPLETED & update DB with output info
            manager.complete_task(
                header_id,
                success=True,
                result_location=result_path,
                result_format=result_format
            )

            return result

        return wrapper

    return decorator


def _extract_call_signature(func, args, kwargs):
    """
    Capture the function's signature into a dictionary mapping parameter names to values.
    This dictionary is stored in the DB to detect duplicate calls.
    """
    import inspect
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    return dict(bound.arguments)


def _save_to_disk(result, base_dir, task_key, header_id):
    """
    Saves `result` in a human-readable format:
      - If `result` is a pandas DataFrame, save as CSV.
      - If `result` is a dict, save as JSON.
      - If `result` is a list or set, convert set to a sorted list and save as JSON.
      - If `result` is an int, float, or string, save as plain text (TXT).
      - If it's something else, attempt JSON; if that fails, store a string representation.
    Returns a tuple: (full_path, file_format).
    """
    timestamp = int(time.time() * 1000)

    # 1. Pandas DataFrame → CSV
    if isinstance(result, pd.DataFrame):
        file_format = 'csv'
        filename = f"{task_key}_{header_id}_{timestamp}.csv"
        full_path = os.path.join(base_dir, filename)
        result.to_csv(full_path, index=False)
        return full_path, file_format

    # 2. Dict → JSON
    elif isinstance(result, dict):
        file_format = 'json'
        filename = f"{task_key}_{header_id}_{timestamp}.json"
        full_path = os.path.join(base_dir, filename)
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        return full_path, file_format

    # 3. List or Set → JSON (convert set to a sorted list)
    elif isinstance(result, (list, set)):
        file_format = 'json'
        filename = f"{task_key}_{header_id}_{timestamp}.json"
        full_path = os.path.join(base_dir, filename)
        if isinstance(result, set):
            result = sorted(list(result))
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        return full_path, file_format

    # 4. int, float, or str → TXT
    elif isinstance(result, (int, float, str)):
        file_format = 'txt'
        filename = f"{task_key}_{header_id}_{timestamp}.txt"
        full_path = os.path.join(base_dir, filename)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(str(result))
        return full_path, file_format

    # 5. Otherwise, try JSON-serialize, if it fails store as a generic string
    else:
        file_format = 'json'
        filename = f"{task_key}_{header_id}_{timestamp}_generic.json"
        full_path = os.path.join(base_dir, filename)
        try:
            with open(full_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
        except TypeError:
            # If not JSON-serializable, store a string representation
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(str(result))
        return full_path, file_format


def _load_from_disk(path, file_format):
    """
    Load previously saved data based on the file_format.
    - 'csv' -> DataFrame
    - 'json' -> dict, list, or other JSON-compatible object
    - 'txt' -> raw string
    - fallback -> try JSON, else raw string
    """
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"Expected file at {path} but it doesn't exist.")

    if file_format == 'csv':
        return pd.read_csv(path)
    elif file_format == 'json':
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    elif file_format == 'txt':
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    else:
        with open(path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return f.read()

