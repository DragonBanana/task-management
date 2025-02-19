# mytaskmanager/decorators.py

import json
import logging
import inspect
from functools import wraps
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from .config import load_config
from .model import Base, Task, TaskStatus
from .output import JSONOutputManager
from .exception import TaskInProgressError, TaskFailedError


def task():
    """
    Decorator that reads *all* parameters from the global config.
    No arguments are taken here, because db_url, output_dir, use_cache,
    skip_if_in_progress, project_name, and log_level all come from config.
    """
    # Load entire config
    cfg = load_config()
    db_url = cfg["db_url"]
    output_dir = cfg["output_dir"]
    use_cache = cfg["use_cache"]
    skip_if_in_progress = cfg["skip_if_in_progress"]
    project_name = cfg["project_name"]
    log_level_str = cfg["log_level"]

    # Set up logging
    logger = logging.getLogger("banana_task")
    level = getattr(logging, log_level_str.upper(), logging.INFO)
    logger.setLevel(level)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"=== [Project: {project_name}] Starting task decorator for '{func.__name__}' ===")

            # 1. Gather function parameters and turn them into a JSON string
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            param_dict = dict(bound_args.arguments)
            param_json_str = json.dumps(param_dict, sort_keys=True)
            task_name = func.__name__

            # 2. Connect to the DB
            engine = create_engine(db_url, echo=False)
            Base.metadata.create_all(engine)
            Session = sessionmaker(bind=engine)
            session = Session(expire_on_commit=False)

            # 3. Prepare the JSON output manager for caching
            output_mgr = JSONOutputManager(output_dir)

            try:
                # 4. Find or create the Task record
                existing_task = session.query(Task).filter_by(
                    task_name=task_name,
                    parameters=param_json_str
                ).first()

                if not existing_task:
                    new_task = Task(
                        task_name=task_name,
                        parameters=param_json_str,
                        status=TaskStatus.CREATED,
                        creation_time=datetime.utcnow()
                    )
                    session.add(new_task)
                    try:
                        session.commit()
                        existing_task = new_task  # Insert succeeded
                        logger.info(f"[{project_name}] Created new task record: {task_name} | {param_dict}")
                    except IntegrityError:
                        # Another process inserted the same key first
                        session.rollback()
                        existing_task = session.query(Task).filter_by(
                            task_name=task_name,
                            parameters=param_json_str
                        ).one()
                        logger.info(
                            f"[{project_name}] Task record already existed, fetched existing: {task_name} | {param_dict}")

                # 5. Check if we should skip if it's already in progress
                if skip_if_in_progress and existing_task.status == TaskStatus.RUNNING:
                    msg = (f"[{project_name}] Task '{task_name}' (params={param_dict}) is already RUNNING; "
                           "skip_if_in_progress is True → raising TaskInProgressError")
                    logger.warning(msg)
                    session.close()
                    raise TaskInProgressError(msg)

                # 6. If use_cache and the task is completed, try to load from JSON
                if use_cache and existing_task.status == TaskStatus.COMPLETED:
                    cached_result = output_mgr.load_output(task_name, param_dict)
                    if cached_result is not None:
                        logger.info(f"[{project_name}] Using cached result for '{task_name}' | {param_dict}")
                        session.close()
                        return cached_result
                    else:
                        logger.debug(f"[{project_name}] Completed in DB, but no JSON file found → re-run function.")

                # 7. Mark the task as RUNNING
                existing_task.status = TaskStatus.RUNNING
                if not existing_task.creation_time:
                    existing_task.creation_time = datetime.utcnow()
                session.commit()
                logger.info(f"[{project_name}] Task '{task_name}' is now RUNNING with params={param_dict}")

                # 8. Run the actual function
                start_time = datetime.utcnow()
                try:
                    result = func(*args, **kwargs)
                    end_time = datetime.utcnow()
                except Exception as e:
                    # Mark as FAILED
                    end_time = datetime.utcnow()
                    existing_task.status = TaskStatus.FAILED
                    existing_task.completion_time = end_time
                    existing_task.duration_seconds = int((end_time - existing_task.creation_time).total_seconds())
                    session.commit()

                    err_msg = (f"[{project_name}] Task '{task_name}' (params={param_dict}) "
                               f"failed with error: {e}")
                    logger.exception(err_msg)
                    session.close()
                    raise TaskFailedError(err_msg) from e

                # 9. Save the result to a JSON file
                result_path = output_mgr.save_output(task_name, param_dict, result)

                # 10. Mark COMPLETED in DB
                existing_task.status = TaskStatus.COMPLETED
                existing_task.completion_time = end_time
                existing_task.duration_seconds = int((end_time - existing_task.creation_time).total_seconds())
                existing_task.result_path = result_path
                session.commit()

                logger.info(
                    f"[{project_name}] Task '{task_name}' completed in {existing_task.duration_seconds} sec. "
                    f"Result saved to {result_path}"
                )

                return result

            finally:
                session.close()
                logger.debug(f"[{project_name}] Session closed for '{task_name}'")

        return wrapper
    return decorator
