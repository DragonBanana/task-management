import functools
import inspect
import json
import os
import time
import pandas as pd
from datetime import datetime

import yaml
from exception import SubtaskInProgressException
from config import DEFAULT_DB_URL, DEFAULT_RESULT_DIR
from task_manager import TaskManager


def task(db_url=DEFAULT_DB_URL,
         output_dir=DEFAULT_RESULT_DIR,
         use_cache=True,
         skip_if_in_progress=False):
    """
    Decorator that transforms a function into a "cached" task:

    - If 'use_cache=True' (default), we check if an identical task
      (function name + params) is already COMPLETED. If so, we skip re-execution
      and load from disk. If 'use_cache=False', we always re-run and override.
    - If 'skip_if_in_progress=True', and we detect an identical task is PENDING
      or RUNNING, we raise SubtaskInProgressException. This can be caught by
      a parent task to skip as well.

    The parent task’s decorator also catches SubtaskInProgressException. If a
    child subtask is in progress, we skip the parent too, marking it as a failure
    or "skipped" in the DB.

    :param db_url: SQLAlchemy DB URL (e.g., 'postgresql://user:pass@host:5432/db')
    :param output_dir: Directory where outputs are saved
    :param use_cache: If True, re-use completed tasks; if False, always re-run
    :param skip_if_in_progress: If True, raise SubtaskInProgressException if a
                                matching task is PENDING/RUNNING
    """
    manager = TaskManager(db_url=db_url, echo=False)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 1. Build a unique task_key from the function name
            func_name = func.__name__
            param_dict = _extract_call_signature(func, args, kwargs)
            task_key = func_name

            # Add flags to input_params for DB traceability
            param_dict["_use_cache"] = use_cache
            param_dict["_skip_if_in_progress"] = skip_if_in_progress

            # 2. Build the save directory
            module_folder = func.__module__.replace('.', '_')
            save_dir = os.path.join(output_dir, module_folder, func_name)
            os.makedirs(save_dir, exist_ok=True)

            # 3. If skip_if_in_progress => check if identical task is PENDING or RUNNING
            if skip_if_in_progress:
                in_prog_header = manager.find_incomplete_task(task_key, input_params=param_dict)
                if in_prog_header is not None:
                    # Raise custom exception to signal to parent or caller to skip
                    raise SubtaskInProgressException(
                        f"Task {task_key} is in progress (id={in_prog_header.id}). Skipping."
                    )

            # 4. If use_cache => check if identical task is COMPLETED
            if use_cache:
                completed_header = manager.find_completed_task(task_key, input_params=param_dict)
                if completed_header is not None:
                    print(f"Task already completed: {completed_header}")
                    return _load_from_disk(
                        completed_header.result_location,
                        completed_header.result_format
                    )

            # 5. Otherwise, create a new row in DB
            random_seed = param_dict.get('random_seed', None)
            new_header = manager.create_task(
                task_key=task_key,
                random_seed=random_seed,
                input_params=param_dict,
                result_location=None,
                result_format=None
            )
            header_id = new_header.id

            # Move to RUNNING state
            started = manager.start_task(header_id)
            if not started:
                raise RuntimeError(f"Failed to start task_id={header_id}.")

            # 6. Attempt to run the user function
            try:
                result = func(*args, **kwargs)
            except SubtaskInProgressException as e:
                # Means a child subtask was in progress => skip THIS task too
                print(f"Skipping {task_key} because subtask is in progress: {str(e)}")
                manager.complete_task(
                    header_id,
                    success=False,
                    error_message="Skipped due to subtask in progress."
                )
                return None
            except Exception as ex:
                manager.complete_task(header_id, success=False, error_message=str(ex))
                raise

            # 7. If no return => store a small placeholder
            if result is None:
                result = "No return value from function"

            # 8. Save the result to disk
            result_path, result_format = _save_to_disk(
                result,
                base_dir=save_dir,
                task_key=task_key,
                random_seed=random_seed
            )

            # 9. Mark COMPLETED
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


def _save_to_disk(result, base_dir, task_key, random_seed):
    """
    Saves `result` in a human-readable format:
      - If `result` is a pandas DataFrame, save as CSV.
      - If `result` is a dict, save as JSON.
      - If `result` is a list or set, convert set to a sorted list and save as JSON.
      - If `result` is an int, float, or string, save as plain text (TXT).
      - If it's something else, attempt JSON; if that fails, store a string representation.

    The filename format is:
        {timestamp}_{random_seed}_{task_key}.{ext}

    Returns a tuple: (full_path, file_format).
    """
    timestamp = int(time.time() * 1000)

    def make_filename(ext):
        return f"{timestamp}_{random_seed}_{task_key}.{ext}"

    if isinstance(result, pd.DataFrame):
        file_format = 'csv'
        filename = make_filename('csv')
        full_path = os.path.join(base_dir, filename)
        result.to_csv(full_path, index=False)
        return full_path, file_format

    elif isinstance(result, dict):
        file_format = 'json'
        filename = make_filename('json')
        full_path = os.path.join(base_dir, filename)
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        return full_path, file_format

    elif isinstance(result, (list, set)):
        file_format = 'json'
        filename = make_filename('json')
        full_path = os.path.join(base_dir, filename)
        if isinstance(result, set):
            result = sorted(list(result))
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        return full_path, file_format

    elif isinstance(result, (int, float, str)):
        file_format = 'txt'
        filename = make_filename('txt')
        full_path = os.path.join(base_dir, filename)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(str(result))
        return full_path, file_format

    else:
        file_format = 'json'
        filename = f"{timestamp}_{random_seed}_{task_key}_generic.json"
        full_path = os.path.join(base_dir, filename)
        try:
            with open(full_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
        except TypeError:
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
