# example_usage.py
from banana_task.decorator import task
from banana_task.exception import TaskInProgressError, TaskFailedError

@task()
def my_function(x: int, y: int):
    return x + y

if __name__ == "__main__":
    try:
        result = my_function(5, 7)
        print("Result:", result)
    except TaskInProgressError as e:
        print("Skipped because task is in progress:", e)
    except TaskFailedError as e:
        print("Task failed:", e)