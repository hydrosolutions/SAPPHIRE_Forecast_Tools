from pydantic import BaseModel, ConfigDict, Field, field_validator
from datetime import datetime
from typing import Optional


class TaskBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=200, description="Task title")
    description: Optional[str] = Field(None, max_length=1000, description="Task description")
    completed: bool = Field(default=False, description="Task completion status")

    @field_validator('title')
    @classmethod
    def title_must_not_be_empty(cls, v: str) -> str:
        """Validate title is not just whitespace"""
        if not v or not v.strip():
            raise ValueError('Title cannot be empty or just whitespace')
        return v.strip()

    @field_validator('description')
    @classmethod
    def description_strip_whitespace(cls, v: Optional[str]) -> Optional[str]:
        """Strip whitespace from description"""
        if v:
            return v.strip() if v.strip() else None
        return v


class TaskCreate(TaskBase):
    """Schema for creating a task"""
    pass


class TaskUpdate(BaseModel):
    """Schema for updating a task - all fields optional"""
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    completed: Optional[bool] = None

    @field_validator('title')
    @classmethod
    def title_must_not_be_empty(cls, v: Optional[str]) -> Optional[str]:
        """Validate title is not just whitespace"""
        if v is not None and (not v or not v.strip()):
            raise ValueError('Title cannot be empty or just whitespace')
        return v.strip() if v else v

    @field_validator('description')
    @classmethod
    def description_strip_whitespace(cls, v: Optional[str]) -> Optional[str]:
        """Strip whitespace from description"""
        if v:
            return v.strip() if v.strip() else None
        return v


class Task(TaskBase):
    """Schema for returning a task"""
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TaskList(BaseModel):
    """Schema for paginated task list"""
    tasks: list[Task]
    total: int
    skip: int
    limit: int
