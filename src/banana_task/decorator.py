# mytaskmanager/decorators.py

import hashlib
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

############################################
#           Module-Level Globals           #
############################################

# 1) Load config once
CONFIG = load_config()

DB_URL = CONFIG["db_url"]
OUTPUT_DIR = CONFIG["output_dir"]
USE_CACHE = CONFIG["use_cache"]
SKIP_IF_IN_PROGRESS = CONFIG["skip_if_in_progress"]
PROJECT_NAME = CONFIG["project_name"]
LOG_LEVEL_STR = CONFIG["log_level"]

# 2) Configure the logger once
logger = logging.getLogger("banana_task")
level = getattr(logging, LOG_LEVEL_STR.upper(), logging.INFO)
logging.basicConfig(level = logging.INFO)
logger.setLevel(level)

# 3) Create the global SQLAlchemy engine & session factory
engine = create_engine(DB_URL, echo=False)
Base.metadata.create_all(engine)
SessionFactory = sessionmaker(bind=engine, expire_on_commit=False)

# 4) Reuse a single JSONOutputManager
output_mgr = JSONOutputManager(OUTPUT_DIR)

############################################
#           The Decorator                  #
############################################

def task():
    """
    Decorator that uses module-level globals for config, logging, engine, etc.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"=== [Project: {PROJECT_NAME}] Starting '{func.__name__}' ===")

            # 1. Gather function parameters and turn them into a JSON string & hash
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            param_dict = dict(bound_args.arguments)
            param_json_str = json.dumps(param_dict, sort_keys=True)
            param_hash = hashlib.md5(param_json_str.encode("utf-8")).hexdigest()
            task_name = func.__name__

            logger.debug(
                f"[{PROJECT_NAME}] Function '{func.__name__}' called with "
                f"raw params={param_dict}, json_str={param_json_str}, hash={param_hash}"
            )

            # 2. Open a new DB session (reusing the global engine + session factory)
            session = SessionFactory()

            try:
                # 3. Query for existing Task record by (task_name, parameters_hash, parameters)
                logger.debug(f"[{PROJECT_NAME}] Looking up existing task record in DB.")
                existing_task = session.query(Task).filter_by(
                    task_name=task_name,
                    parameters_hash=param_hash,
                    parameters=param_dict,
                ).first()

                if existing_task:
                    logger.debug(f"[{PROJECT_NAME}] Found existing task: status={existing_task.status}")
                else:
                    logger.debug(f"[{PROJECT_NAME}] No existing task found; attempting to create a new one.")

                # If no existing task, create it
                if not existing_task:
                    new_task = Task(
                        task_name=task_name,
                        parameters_hash=param_hash,
                        parameters=param_dict,
                        status=TaskStatus.CREATED,
                        creation_time=datetime.utcnow()
                    )
                    session.add(new_task)
                    try:
                        session.commit()
                        existing_task = new_task
                        logger.info(
                            f"[{PROJECT_NAME}] Created new task record: {task_name} | params={param_dict}"
                        )
                    except IntegrityError:
                        # Another process or thread inserted the same key
                        logger.debug(
                            f"[{PROJECT_NAME}] IntegrityError - possible concurrency. "
                            f"Rolling back and re-fetching the record."
                        )
                        session.rollback()
                        existing_task = session.query(Task).filter_by(
                            task_name=task_name,
                            parameters_hash=param_hash,
                            parameters=param_dict,
                        ).one()
                        logger.info(
                            f"[{PROJECT_NAME}] Task record already existed (concurrency), "
                            f"fetched existing: {task_name} | {param_dict}"
                        )

                # 4. Check if we should skip if it's already running
                if SKIP_IF_IN_PROGRESS and existing_task.status == TaskStatus.RUNNING:
                    msg = (
                        f"[{PROJECT_NAME}] Task '{task_name}' (params={param_dict}) is RUNNING; "
                        "skip_if_in_progress=True → TaskInProgressError raised."
                    )
                    logger.warning(msg)
                    session.close()
                    raise TaskInProgressError(msg)

                # 5. If USE_CACHE and the task is completed, try to load from JSON
                if USE_CACHE and existing_task.status == TaskStatus.COMPLETED:
                    logger.debug(f"[{PROJECT_NAME}] Task is COMPLETED in DB, checking JSON cache.")
                    cached_result = output_mgr.load_output(task_name, param_dict)
                    if cached_result is not None:
                        logger.info(
                            f"[{PROJECT_NAME}] Cache hit! Returning cached result for '{task_name}' | {param_dict}"
                        )
                        session.close()
                        return cached_result
                    else:
                        logger.debug(
                            f"[{PROJECT_NAME}] Task COMPLETED in DB, but no JSON found. "
                            "Re-running the function."
                        )

                # 6. Mark the task as RUNNING
                existing_task.status = TaskStatus.RUNNING
                if not existing_task.creation_time:
                    existing_task.creation_time = datetime.utcnow()
                session.commit()
                logger.info(
                    f"[{PROJECT_NAME}] Task '{task_name}' set to RUNNING with params={param_dict}"
                )

                # 7. Run the actual function
                start_time = datetime.utcnow()
                try:
                    result = func(*args, **kwargs)
                    end_time = datetime.utcnow()
                except Exception as e:
                    # Mark as FAILED
                    end_time = datetime.utcnow()
                    existing_task.status = TaskStatus.FAILED
                    existing_task.completion_time = end_time
                    existing_task.duration_seconds = int(
                        (end_time - existing_task.creation_time).total_seconds()
                    )
                    session.commit()

                    err_msg = (
                        f"[{PROJECT_NAME}] Task '{task_name}' (params={param_dict}) "
                        f"failed with error: {e}"
                    )
                    logger.exception(err_msg)
                    session.close()
                    raise TaskFailedError(err_msg) from e

                # 8. Save the result to a JSON file
                logger.debug(
                    f"[{PROJECT_NAME}] Function '{task_name}' succeeded. Saving output to JSON."
                )
                result_path = output_mgr.save_output(task_name, param_dict, result)

                # 9. Mark COMPLETED in DB
                existing_task.status = TaskStatus.COMPLETED
                existing_task.completion_time = end_time
                existing_task.duration_seconds = int(
                    (end_time - existing_task.creation_time).total_seconds()
                )
                existing_task.result_path = result_path
                session.commit()

                logger.info(
                    f"[{PROJECT_NAME}] Task '{task_name}' completed in "
                    f"{existing_task.duration_seconds} sec. Result cached at {result_path}"
                )

                return result

            finally:
                session.close()
                logger.debug(f"[{PROJECT_NAME}] Session closed for '{task_name}'")

        return wrapper
    return decorator
