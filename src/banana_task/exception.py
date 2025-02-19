# exceptions.py

class TaskInProgressError(Exception):
    """
    Raised when a task is skipped because it is already RUNNING
    and skip_if_in_progress=True.
    """
    pass

class TaskFailedError(Exception):
    """
    Raised when the decorated function fails, or the task ends in FAILED state.
    """
    pass
