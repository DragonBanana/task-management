# src/mytaskmanager/decorators.py
import json
import inspect
from functools import wraps
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import load_config
from model import Base, Task, TaskStatus
from output import JSONOutputManager


def task(
        db_url=None,
        output_dir=None,
        use_cache=None,
        skip_if_in_progress=None
):
    """
    Decorator that converts a function into a tracked task.

    If any of the arguments are None, we fall back to the library's loaded config.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 1. Load global config and override with function arguments (if not None)
            global_cfg = load_config()
            effective_db_url = db_url or global_cfg["db_url"]
            effective_output_dir = output_dir or global_cfg["output_dir"]
            effective_use_cache = use_cache if use_cache is not None else global_cfg["use_cache"]
            effective_skip_if_in_progress = (skip_if_in_progress
                                             if skip_if_in_progress is not None
                                             else global_cfg["skip_if_in_progress"])

            # 2. Derive param_dict from function signature
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            param_dict = dict(bound.arguments)  # e.g. {"x": 10, "y": "foo"}

            # 3. Convert param_dict to a JSON string to store in DB
            param_json_str = json.dumps(param_dict, sort_keys=True)
            task_name = func.__name__
            creation_time = datetime.utcnow()

            # 4. Prepare DB session
            engine = create_engine(effective_db_url, echo=False)
            Base.metadata.create_all(engine)
            Session = sessionmaker(bind=engine)
            session = Session()

            # 5. Prepare JSONOutputManager
            output_mgr = JSONOutputManager(effective_output_dir)

            try:
                # 6. Find or create Task record
                existing_task = session.query(Task).filter_by(
                    task_name=task_name,
                    parameters=param_json_str
                ).first()

                if not existing_task:
                    existing_task = Task(
                        task_name=task_name,
                        parameters=param_json_str,
                        status=TaskStatus.CREATED,
                        creation_time=creation_time
                    )
                    session.add(existing_task)
                    session.commit()

                # 7. Skip if in progress
                if effective_skip_if_in_progress and existing_task.status == TaskStatus.RUNNING:
                    print(f"[task] Skipping '{task_name}' - already running.")
                    session.close()
                    return None

                # 8. If use_cache & COMPLETED, try to load from JSON
                if effective_use_cache and existing_task.status == TaskStatus.COMPLETED:
                    cached_result = output_mgr.load_output(task_name, param_dict)
                    if cached_result is not None:
                        print(f"[task] Using cached result for '{task_name}'.")
                        session.close()
                        return cached_result

                # 9. Update to RUNNING
                existing_task.status = TaskStatus.RUNNING
                if existing_task.creation_time is None:
                    existing_task.creation_time = creation_time
                session.commit()

                # 10. Run the function
                start_time = datetime.utcnow()
                try:
                    result = func(*args, **kwargs)
                    end_time = datetime.utcnow()
                except Exception as e:
                    # Mark FAILED
                    end_time = datetime.utcnow()
                    existing_task.status = TaskStatus.FAILED
                    existing_task.completion_time = end_time
                    existing_task.duration_seconds = int((end_time - existing_task.creation_time).total_seconds())
                    session.commit()
                    session.close()
                    raise

                # 11. Save to JSON file
                result_path = output_mgr.save_output(task_name, param_dict, result)

                # 12. Mark COMPLETED
                existing_task.status = TaskStatus.COMPLETED
                existing_task.completion_time = end_time
                duration = (end_time - existing_task.creation_time).total_seconds()
                existing_task.duration_seconds = int(duration)
                existing_task.result_path = result_path
                session.commit()

                return result
            finally:
                session.close()

        return wrapper

    return decorator
