import enum
import json
from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    BigInteger,
    Enum,
    CheckConstraint
)
from sqlalchemy.dialects.postgresql import JSONB  # If you use PostgreSQL
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class TaskStatus(enum.Enum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class Task(Base):
    __tablename__ = "tasks"

    # Composite primary key: (task_name, serialized_parameters)
    task_name = Column(String(255), primary_key=True)
    parameters = Column(JSONB, primary_key=True)

    status = Column(Enum(TaskStatus), nullable=False)
    creation_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    completion_time = Column(DateTime, nullable=True)
    duration_seconds = Column(BigInteger, nullable=True)
    result_path = Column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('created','running','completed','failed')",
            name="tasks_status_check"
        ),
    )

    def __repr__(self):
        return (
            f"<Task(task_name='{self.task_name}', "
            f"params='{self.parameters}', "
            f"status='{self.status.value}')>"
        )
