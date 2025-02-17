# models.py

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, DateTime,
    ForeignKey, Text
)
from sqlalchemy.dialects.sqlite import JSON as SqliteJSON
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
    input_params = Column(SqliteJSON, nullable=True)
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
