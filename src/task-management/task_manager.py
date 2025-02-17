# task_manager.py

from contextlib import contextmanager
from datetime import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from models import Base, TaskHeader, TaskState

class TaskManager:
    _engine = None
    _SessionFactory = None

    def __init__(
        self,
        # For PostgreSQL:
        # Format: "postgresql://username:password@host:port/dbname"
        db_url='postgresql://user:password@localhost:5432/banana-mydb',
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    ):
        """
        :param db_url: The PostgreSQL DB URL, e.g. "postgresql://user:pass@localhost:5432/mydb"
        :param echo: If True, SQLAlchemy will log all SQL statements
        :param pool_size: The number of connections to keep open inside the pool.
        :param max_overflow: Number of extra connections allowed above pool_size.
        :param pool_timeout: Number of seconds to wait for a connection from the pool.
        :param pool_recycle: Number of seconds after which a connection is automatically recycled.
        """
        if TaskManager._engine is None:
            # Create the engine with PostgreSQL + pooling settings
            TaskManager._engine = create_engine(
                db_url,
                echo=echo,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle
            )
            # Create tables if they don't exist yet
            Base.metadata.create_all(TaskManager._engine)

        if TaskManager._SessionFactory is None:
            # Use a scoped_session so each thread has its own Session,
            # but all sessions share the same engine & connection pool.
            TaskManager._SessionFactory = scoped_session(
                sessionmaker(bind=TaskManager._engine, expire_on_commit=False)
            )

    @contextmanager
    def session_scope(self):
        """
        Provide a transactional scope around a series of operations.
        Every operation within this context is part of a single transaction.

        - If no exception occurs, the transaction is committed automatically.
        - If an exception is raised, the transaction is rolled back.
        """
        session = TaskManager._SessionFactory()
        try:
            with session.begin():
                yield session
        finally:
            # Whether committed or rolled back, the session must be closed.
            session.expunge_all()
            session.close()

    def create_task(self, task_key, random_seed=None, input_params=None,
                    result_location=None, result_format=None):
        """
        Create a new task (header+state) with status=PENDING, within a transaction.
        """
        with self.session_scope() as session:
            header = TaskHeader(
                task_key=task_key,
                random_seed=random_seed,
                input_params=input_params,
                result_location=result_location,
                result_format=result_format
            )
            session.add(header)
            session.flush()  # to assign header.id

            state = TaskState(
                task_header_id=header.id,
                status='PENDING'
            )
            session.add(state)

            session.expunge(header)
            return header

    def start_task(self, header_id):
        """
        Transition task from PENDING -> RUNNING, set start_time.
        """
        with self.session_scope() as session:
            state = session.query(TaskState).filter_by(task_header_id=header_id).first()
            if state and state.status == 'PENDING':
                state.status = 'RUNNING'
                state.start_time = datetime.utcnow()
                return True
            return False

    def complete_task(self, header_id, success=True, error_message=None,
                      result_location=None, result_format=None):
        """
        Mark a task as COMPLETED or FAILED, store end_time, duration, etc.
        """
        with self.session_scope() as session:
            state = session.query(TaskState).filter_by(task_header_id=header_id).first()
            if not state or state.status not in ('RUNNING', 'PENDING'):
                return False

            header = session.query(TaskHeader).get(header_id)

            state.end_time = datetime.utcnow()
            if state.start_time:
                delta = (state.end_time - state.start_time).total_seconds()
                state.duration_seconds = delta

            if success:
                state.status = 'COMPLETED'
            else:
                state.status = 'FAILED'
                state.error_message = error_message

            if result_location:
                header.result_location = result_location
            if result_format:
                header.result_format = result_format

            return True

    def find_incomplete_task(self, task_key, input_params=None):
        """
        Returns a TaskHeader that matches (task_key, input_params)
        and is in PENDING or RUNNING state, or None if not found.
        """
        with self.session_scope() as session:
            query = (
                session.query(TaskHeader)
                .join(TaskState)
                .filter(
                    TaskHeader.task_key == task_key,
                    TaskState.status.in_(["PENDING", "RUNNING"])
                )
            )
            if input_params is not None:
                query = query.filter(TaskHeader.input_params == input_params)

            header = query.first()
            if header:
                session.expunge(header)
            return header

    def find_completed_task(self, task_key, input_params=None):
        """
        Return a COMPLETED TaskHeader with the given task_key & input_params.
        """
        with self.session_scope() as session:
            query = (
                session.query(TaskHeader)
                .join(TaskState)
                .filter(
                    TaskHeader.task_key == task_key,
                    TaskState.status == 'COMPLETED'
                )
            )
            if input_params is not None:
                query = query.filter(TaskHeader.input_params == input_params)

            header = query.first()
            if header:
                session.expunge(header)
            return header

    def get_task(self, header_id):
        """
        Retrieve a TaskHeader + TaskState by ID.
        """
        with self.session_scope() as session:
            header = session.query(TaskHeader).filter_by(id=header_id).first()
            if header:
                session.expunge(header)
            return header

    def list_tasks(self, status=None):
        """
        Retrieve all tasks or tasks by status.
        """
        with self.session_scope() as session:
            query = session.query(TaskHeader).join(TaskState)
            if status:
                query = query.filter(TaskState.status == status)
            results = query.all()
            for r in results:
                session.expunge(r)
            return results
