# Project: Task Management Framework

**Description**  
This project is a simple yet flexible framework for managing tasks in a reproducible, automated manner. It uses a dedicated manager class to handle database interactions and a decorator to wrap user-defined functions, automatically caching results on disk for reproducibility. An optional config file can store database credentials, paths, and other settings.

The framework supports:
- Relational databases (SQLite, PostgreSQL, etc.) via SQLAlchemy.
- Automatic caching of function outputs (DataFrame, dict, string, etc.) to files.
- Task state management (PENDING, RUNNING, COMPLETED, FAILED).
- Advanced features for skipping tasks if subtasks are in progress.

---

## Installation from GitHub

To make the project installable via pip from GitHub, ensure the repository has the necessary build configuration (for example, a setup file or pyproject file). Then install with:

Use a pip command referencing your GitHub URL, such as:
- A direct link to `master` or `main`.
- A specific commit or tag.

---

## Usage

1. **Setting up a database**  
   By default, the code references a SQLite database or a configured value for the database URL. You can also use PostgreSQL by specifying the correct connection string.

2. **Configuring defaults**  
   Use either a Python config or YAML file to set variables like `DEFAULT_DB_URL` and `DEFAULT_RESULT_DIR`.

3. **Defining a task**  
   Decorate your function with the provided decorator. The first time it is called with certain parameters, it will be executed and its results cached. Subsequent calls with identical parameters will load the cached output, unless configured otherwise.

4. **Caching and re-using results**  
   If the same task is invoked again with the same parameters, the framework detects a previously completed task and automatically loads stored data instead of re-running the function.

---

## Configuration

An optional external file (such as YAML) can store:
- Database credentials and host details.
- Output directories and other application-level parameters.

You can load this file at runtime, build the database URL, and pass it to the manager or decorator.

---

## Task States & Database

The framework tracks:
- **Immutable metadata** (task key, random seed, input parameters) in one table.
- **Mutable state** (PENDING, RUNNING, COMPLETED, FAILED) in a related table.

---

## Local File Caching

When tasks complete, their outputs are saved in a directory structured by module and function name. Each file is named based on a timestamp, an optional random seed, and the task key. On re-invocation with the same parameters, the framework looks up the cached file.

---

## Advanced Features

- **Skipping tasks if subtasks are in progress**: A parent task can choose to skip its own execution if a child task is still running.
- **Overriding cached results**: You can disable cache usage and force the function to re-run.

---

## Development & Contribution

1. **Create a virtual environment** and install dependencies.  
2. **Run tests** if available.  
3. **Add features and bug fixes**, then contribute via pull requests.  

---

## License

Include a license statement if open-sourcing (e.g., MIT, Apache 2.0, etc.).

---

## Contact

If you have questions or need support, open an issue in the repository or contact the maintainer via email.
