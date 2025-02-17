# models.py

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, DateTime,
    ForeignKey, Text
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class TaskHeader(Base):
    """
    Immutable metadata for the task (like random seed, input_params).
    """
    __tablename__ = 'task_headers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_key = Column(String(255), nullable=False, unique=False)
    random_seed = Column(Integer, nullable=True)

    # Replace the Sqlite-specific JSON column with PostgreSQL's JSONB
    input_params = Column(JSONB, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    result_location = Column(String(500), nullable=True)
    result_format = Column(String(50), nullable=True)

    # Eagerly load the associated TaskState using joined loading
    state = relationship(
        'TaskState',
        uselist=False,
        back_populates='task_header',
        lazy='joined'
    )

    def __repr__(self):
        """
        Official string representation, for debugging.
        Shows the class name and key fields.
        """
        return (f"<TaskHeader(id={self.id}, "
                f"task_key={self.task_key!r}, "
                f"random_seed={self.random_seed}, "
                f"input_params={self.input_params}, "
                f"created_at={self.created_at}, "
                f"result_location={self.result_location!r}, "
                f"result_format={self.result_format!r})>")

    def __str__(self):
        """
        User-friendly string representation, often used in print statements.
        Could include fewer or more readable details than __repr__.
        """
        return (f"TaskHeader["
                f"id={self.id}, "
                f"key={self.task_key}, "
                f"seed={self.random_seed}, "
                f"created={self.created_at}]")

class TaskState(Base):
    """
    Mutable state of the task: status, times, error messages, etc.
    """
    __tablename__ = 'task_states'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_header_id = Column(Integer, ForeignKey('task_headers.id'), nullable=False)
    status = Column(String(50), default='PENDING')  # e.g. PENDING, RUNNING, COMPLETED, FAILED
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Eagerly load the associated TaskHeader using joined loading if needed
    task_header = relationship(
        'TaskHeader',
        back_populates='state',
        lazy='joined'
    )
