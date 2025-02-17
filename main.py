# main.py

import pandas as pd
from decorators import task

@task(db_url='sqlite:///example_tasks.db', output_dir='my_outputs')
def my_experiment(a, b, random_seed=42):
    # Example: returns a DataFrame
    import numpy as np
    data = {
        'col1': np.random.rand(a),
        'col2': [b]*a
    }
    return pd.DataFrame(data)

@task(db_url='sqlite:///example_tasks.db', output_dir='my_outputs')
def compute_something(x, random_seed=123):
    # Example: returns a dict
    return {"value": x**2, "seed": random_seed}

def main():
    # First call: not cached, so the function runs
    df_result = my_experiment(5, 10, random_seed=999)
    print("DF result shape:", df_result.shape)

    # Second call with same args:
    #   -> detects an existing COMPLETED task
    #   -> reloads from disk, skipping re-computation
    df_result2 = my_experiment(5, 10, random_seed=999)
    print("DF result2 shape (cached):", df_result2.shape)

    # Different function, returns a dict
    dict_res = compute_something(4, random_seed=777)
    print("Dict result:", dict_res)

    # Re-call with same args -> loads from disk
    dict_res2 = compute_something(4, random_seed=777)
    print("Dict result2 (cached):", dict_res2)

if __name__ == "__main__":
    main()
