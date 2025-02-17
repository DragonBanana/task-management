from decorators import task
from exception import SubtaskInProgressException

@task(skip_if_in_progress=True)
def B(x):
    # Some logic that might take a while
    import time
    time.sleep(10)
    return x*x

@task(skip_if_in_progress=True)
def A(x):
    # A calls B, but if B is in progress, B's decorator
    # raises SubtaskInProgressException => skip A
    valB = B(x)
    return valB + 1

def main():
    # Suppose we run B(5) in one thread => it's in progress
    # Another thread calls A(5) => sees B(5) in progress => skip A
    rA = A(6)
    print("A(5) result:", rA)

if __name__ == "__main__":
    main()