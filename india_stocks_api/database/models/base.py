"""
Base model class for database models
"""

from abc import ABC
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class BaseModel(ABC):
    """Base model with common fields"""

    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        """Initialize timestamps if not provided"""
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

    def to_dict(self) -> dict:
        """Convert model to dictionary"""
        result = {}
        for key, value in self.__dict__.items():
            if value is not None:
                if isinstance(value, datetime):
                    result[key] = value.isoformat()
                else:
                    result[key] = value
        return result

    @classmethod
    def from_dict(cls, data: dict):
        """Create model instance from dictionary"""
        # Remove None values and convert datetime strings
        filtered_data = {}
        for key, value in data.items():
            if value is not None:
                if key in ["created_at", "updated_at"] and isinstance(value, str):
                    filtered_data[key] = datetime.fromisoformat(value)
                else:
                    filtered_data[key] = value

        return cls(**filtered_data)
