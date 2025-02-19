"""
json_output_manager.py

Always store task outputs as a JSON file. The filename is composed of
(task_name + parameters). No hashes or numeric IDs are used.

Usage Example:
    manager = JSONOutputManager(output_dir="./results")
    manager.save_output("train_model", {"lr": 0.01, "epochs": 10}, result_object)
    loaded_result = manager.load_output("train_model", {"lr": 0.01, "epochs": 10})
"""

import os
import re
import json
import pandas as pd
from typing import Any, Union


class JSONOutputManager:
    """
    A manager that saves each task's output as a separate JSON file.
    The JSON filename is derived from (task_name, parameters).
    """

    def __init__(self, output_dir: str):
        """
        :param output_dir: Directory in which to store JSON files.
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def save_output(self, task_name: str, parameters: dict, result: Any) -> str:
        """
        Save the `result` to a JSON file whose name is derived from
        (task_name, parameters). Returns the file path.

        Rules for serialization:
          - If `result` is a pandas DataFrame, it is converted to a list-of-records JSON.
          - If `result` is a dict, list, or primitive, store it directly as JSON.
          - If `result` references an image/video path (string), store that string as-is.
        """
        filename = self._make_filename(task_name, parameters)
        file_path = os.path.join(self.output_dir, filename)

        # Convert the result to a JSON-serializable form
        serializable_result = self._make_json_serializable(result)

        # Wrap the data in a small structure with additional info if desired
        content = {
            "task_name": task_name,
            "parameters": parameters,
            "result": serializable_result
        }

        # Write to JSON file (overwrites if it already exists)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=2)

        return file_path

    def load_output(self, task_name: str, parameters: dict) -> Union[Any, None]:
        """
        Load the result from the JSON file (if it exists) corresponding to
        (task_name, parameters). Returns the deserialized object, or None if not found.
        """
        filename = self._make_filename(task_name, parameters)
        file_path = os.path.join(self.output_dir, filename)

        if not os.path.exists(file_path):
            return None

        with open(file_path, "r", encoding="utf-8") as f:
            content = json.load(f)

        # We have a JSON-serializable structure. Reconstruct special cases if needed.
        raw_result = content.get("result", None)
        return self._restore_from_json(raw_result)

    def _make_filename(self, task_name: str, parameters: dict) -> str:
        """
        Create a filename based on (task_name + parameters).
        We must ensure the filename is safe for the filesystem:
          - Serialize `parameters` as JSON,
          - Remove or replace special characters.

        Note: Because we do NOT use hashes or numeric IDs, there's a risk
        of extremely long filenames if `parameters` is large. This is a
        simplistic approach.
        """
        # Convert parameters to a stable JSON string
        param_str = json.dumps(parameters, sort_keys=True)
        # Sanitize for filesystem
        sanitized_params = re.sub(r"[^a-zA-Z0-9_\-]+", "_", param_str)
        sanitized_task_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", task_name)

        # Combine the two
        filename = f"{sanitized_task_name}__{sanitized_params}.json"
        # Example: train_model__{"epochs"_10_"lr"_0_01_}.json  => train_model____epochs__10__lr__0__01__.json
        return filename[:255]  # Trim if too long (some filesystems limit length)

    def _make_json_serializable(self, data: Any) -> Any:
        """
        Convert `data` into a form that can be written as JSON:
          - If it's a pandas DataFrame, convert to records (list of dicts).
          - If it's a dict/list/primitive, return as-is (assuming it's already JSON-friendly).
          - If it's a string referencing an image or video, store as-is.
          - Otherwise, attempt best-effort conversion or raise an error.
        """
        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to a list of row dicts
            return data.to_dict(orient="records")

        # If it's a dict, list, int, float, bool, or string,
        # it's usually JSON-serializable by default, unless it contains nested objects.
        # We'll do a simple test dump to catch errors:
        try:
            json.dumps(data)  # Test if it's already serializable
            return data
        except TypeError:
            # If it's not natively serializable, represent it as a string fallback
            return str(data)

    def _restore_from_json(self, raw_result: Any) -> Any:
        """
        Reverse part of the _make_json_serializable logic if needed.
        Here we do minimal re-construction. For example, if we see
        a list of dicts, it might have come from a DataFrame,
        but we can't automatically re-infer that it should be a DataFrame
        unless we store extra metadata.

        For images/videos (paths), we stored them as strings, so we just return the string.
        """
        # This method is minimal. If you want to automatically
        # convert lists-of-dicts back to DataFrames, you need extra markers
        # in _make_json_serializable (e.g., store a special key).
        return raw_result
