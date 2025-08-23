from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class SourceType(Enum):
    LOCAL = "local"
    S3 = "s3"
    DATABASE = "database"


class FileType(Enum):
    PANDAS_DF = "pandas_df"
    DICT = "dict"
    PICKLE = "pickle"
    JOBLIB = "joblib"


@dataclass
class SourceInfo:
    """Single configuration class for all sources"""

    source_type: Optional[SourceType] = None
    file_type: Optional[FileType] = None
    path: Optional[str] = None
    query: Optional[str] = None

    # Common properties
    name: Optional[str] = None

    # Local file properties
    base_path: Optional[str] = None
    allowed_extensions: Optional[list[str]] = None

    # S3 properties
    aws_profile: Optional[str] = None
    region: Optional[str] = None

    # Database properties
    connection_string: Optional[str] = None
    pool_size: int = 5
    max_overflow: int = 10

    # Snowflake properties
    user: Optional[str] = None
    password: Optional[str] = None
    account: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None


class BaseIO(ABC):
    @abstractmethod
    def load(self, **kwargs):
        pass

    @abstractmethod
    def write(self, **kwargs):
        pass
