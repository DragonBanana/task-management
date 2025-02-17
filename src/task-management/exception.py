class SubtaskInProgressException(Exception):
    """
    Raised when a subtask is discovered to be in progress (PENDING/RUNNING),
    and we prefer skipping the current task rather than waiting.
    """
    pass
