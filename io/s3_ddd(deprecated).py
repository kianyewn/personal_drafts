"""
S3 Data Access Layer following Domain Driven Design principles.
This module provides domain-focused abstractions for S3 operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Protocol, runtime_checkable
import logging

from io_base import BaseIO, FileType, SourceInfo

# Domain-specific exceptions
class UnsupportedFileTypeError(Exception):
    """Raised when attempting to process an unsupported file type."""
    pass

class DataLoadError(Exception):
    """Raised when data loading fails."""
    pass

class DataWriteError(Exception):
    """Raised when data writing fails."""
    pass

class S3ConnectionError(Exception):
    """Raised when S3 connection fails."""
    pass

# Domain interfaces (contracts)
@runtime_checkable
class DataSerializer(Protocol):
    """Protocol defining the contract for data serialization."""
    
    def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        ...
    
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to data."""
        ...

@runtime_checkable
class FileTypeDetector(Protocol):
    """Protocol defining the contract for file type detection."""
    
    def detect_file_type(self, path: str) -> FileType:
        """Detect file type from path."""
        ...

@runtime_checkable
class S3Connection(Protocol):
    """Protocol defining the contract for S3 connection."""
    
    def open(self, path: str, mode: str):
        """Open a file-like object for the given path."""
        ...

# Domain value objects
class S3Path:
    """Value object representing an S3 path."""
    
    def __init__(self, path: str):
        if not path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")
        self._path = path
    
    @property
    def path(self) -> str:
        return self._path
    
    @property
    def bucket(self) -> str:
        return self._path.split('/')[2]
    
    @property
    def key(self) -> str:
        return '/'.join(self._path.split('/')[3:])
    
    def __str__(self) -> str:
        return self._path
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, S3Path):
            return False
        return self._path == other._path
    
    def __hash__(self) -> int:
        return hash(self._path)

# Domain services
class FileTypeDetectionService:
    """Domain service for detecting file types from paths."""
    
    @staticmethod
    def detect_file_type(path: str) -> FileType:
        """Detect file type based on file extension."""
        if path.endswith(".csv"):
            return FileType.PANDAS_DF
        elif path.endswith(".parquet"):
            return FileType.PANDAS_DF
        elif path.endswith(".json"):
            return FileType.DICT
        elif path.endswith(".pickle"):
            return FileType.PICKLE
        elif path.endswith(".joblib"):
            return FileType.JOBLIB
        else:
            raise UnsupportedFileTypeError(f"Unsupported file type: {path}")

class S3ConnectionService:
    """Domain service for managing S3 connections."""
    
    def __init__(self, s3fs_connection=None):
        self._s3fs = s3fs_connection
        self._logger = logging.getLogger(__name__)
    
    def get_connection(self) -> S3Connection:
        """Get S3 connection, creating one if necessary."""
        if self._s3fs is None:
            raise S3ConnectionError("S3 connection not configured")
        return self._s3fs
    
    def validate_connection(self) -> bool:
        """Validate that S3 connection is working."""
        try:
            # Basic connection test
            return self._s3fs is not None
        except Exception as e:
            self._logger.error(f"S3 connection validation failed: {e}")
            return False

# Infrastructure adapters (implement domain interfaces)
class PandasDataSerializer:
    """Infrastructure adapter for pandas DataFrame serialization."""
    
    def serialize(self, data, **kwargs) -> bytes:
        """Serialize pandas DataFrame to bytes."""
        import pandas as pd
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Data must be a pandas DataFrame")
        
        # This is a simplified version - in practice you'd handle different formats
        if kwargs.get('format') == 'csv':
            return data.to_csv(index=False).encode('utf-8')
        elif kwargs.get('format') == 'parquet':
            return data.to_parquet(index=False)
        else:
            return data.to_parquet(index=False)
    
    def deserialize(self, data: bytes, **kwargs) -> Any:
        """Deserialize bytes to pandas DataFrame."""
        import pandas as pd
        if kwargs.get('format') == 'csv':
            return pd.read_csv(data.decode('utf-8'))
        elif kwargs.get('format') == 'parquet':
            return pd.read_parquet(data)
        else:
            return pd.read_parquet(data)

class JsonDataSerializer:
    """Infrastructure adapter for JSON serialization."""
    
    def serialize(self, data, **kwargs) -> bytes:
        """Serialize data to JSON bytes."""
        import json
        return json.dumps(data, **kwargs).encode('utf-8')
    
    def deserialize(self, data: bytes, **kwargs) -> Any:
        """Deserialize JSON bytes to data."""
        import json
        return json.loads(data.decode('utf-8'), **kwargs)

class PickleDataSerializer:
    """Infrastructure adapter for pickle serialization."""
    
    def serialize(self, data, **kwargs) -> bytes:
        """Serialize data to pickle bytes."""
        import pickle
        return pickle.dumps(data, **kwargs)
    
    def deserialize(self, data: bytes, **kwargs) -> Any:
        """Deserialize pickle bytes to data."""
        import pickle
        return pickle.loads(data, **kwargs)

class JoblibDataSerializer:
    """Infrastructure adapter for joblib serialization."""
    
    def serialize(self, data, **kwargs) -> bytes:
        """Serialize data to joblib bytes."""
        import joblib
        return joblib.dumps(data, **kwargs)
    
    def deserialize(self, data: bytes, **kwargs) -> Any:
        """Deserialize joblib bytes to data."""
        import joblib
        return joblib.loads(data, **kwargs)

# Domain entities
class S3DataRepository:
    """Domain entity representing an S3 data repository."""
    
    def __init__(self, connection_service: S3ConnectionService):
        self.connection_service = connection_service
        self.file_type_detector = FileTypeDetectionService()
        self._logger = logging.getLogger(__name__)
        
        # Strategy pattern for different serializers
        self._serializers = {
            FileType.PANDAS_DF: PandasDataSerializer(),
            FileType.DICT: JsonDataSerializer(),
            FileType.PICKLE: PickleDataSerializer(),
            FileType.JOBLIB: JoblibDataSerializer(),
        }
    
    def _get_serializer(self, file_type: FileType) -> DataSerializer:
        """Get appropriate serializer for file type."""
        serializer = self._serializers.get(file_type)
        if serializer is None:
            raise UnsupportedFileTypeError(f"No serializer found for file type: {file_type}")
        return serializer
    
    def _get_file_type_from_path(self, path: str) -> FileType:
        """Get file type from path using domain service."""
        return self.file_type_detector.detect_file_type(path)
    
    def load(self, path: str, **kwargs) -> Any:
        """Load data from S3 using domain logic."""
        try:
            s3_path = S3Path(path)
            file_type = self._get_file_type_from_path(path)
            serializer = self._get_serializer(file_type)
            
            connection = self.connection_service.get_connection()
            with connection.open(s3_path.path, "rb") as f:
                data_bytes = f.read()
            
            return serializer.deserialize(data_bytes, **kwargs)
            
        except Exception as e:
            self._logger.error(f"Failed to load data from {path}: {e}")
            raise DataLoadError(f"Failed to load data from {path}: {e}")
    
    def write(self, data: Any, path: str, **kwargs) -> None:
        """Write data to S3 using domain logic."""
        try:
            s3_path = S3Path(path)
            file_type = self._get_file_type_from_path(path)
            serializer = self._get_serializer(file_type)
            
            # Get format from path extension for pandas
            if file_type == FileType.PANDAS_DF:
                if path.endswith('.csv'):
                    kwargs['format'] = 'csv'
                elif path.endswith('.parquet'):
                    kwargs['format'] = 'parquet'
            
            data_bytes = serializer.serialize(data, **kwargs)
            
            connection = self.connection_service.get_connection()
            with connection.open(s3_path.path, "wb") as f:
                f.write(data_bytes)
                
        except Exception as e:
            self._logger.error(f"Failed to write data to {path}: {e}")
            raise DataWriteError(f"Failed to write data to {path}: {e}")

# Application service (orchestrates domain objects)
class S3DataService:
    """Application service that orchestrates S3 data operations."""
    
    def __init__(self, s3fs_connection=None):
        self.connection_service = S3ConnectionService(s3fs_connection)
        self.repository = S3DataRepository(self.connection_service)
        self._logger = logging.getLogger(__name__)
    
    def load_data(self, s3_path: str, aws_profile_name: str = None, **kwargs) -> Any:
        """Load data from S3 with proper error handling."""
        try:
            # Validate connection first
            if not self.connection_service.validate_connection():
                raise S3ConnectionError("S3 connection is not valid")
            
            return self.repository.load(s3_path, **kwargs)
            
        except Exception as e:
            self._logger.error(f"Data loading failed: {e}")
            raise
    
    def write_data(self, data: Any, s3_path: str, aws_profile_name: str = None, **kwargs) -> None:
        """Write data to S3 with proper error handling."""
        try:
            # Validate connection first
            if not self.connection_service.validate_connection():
                raise S3ConnectionError("S3 connection is not valid")
            
            self.repository.write(data, s3_path, **kwargs)
            
        except Exception as e:
            self._logger.error(f"Data writing failed: {e}")
            raise
    
    def validate_path(self, s3_path: str) -> bool:
        """Validate S3 path format."""
        try:
            S3Path(s3_path)
            return True
        except ValueError:
            return False

# Factory for creating S3 data services
class S3DataServiceFactory:
    """Factory for creating S3 data services with different configurations."""
    
    @staticmethod
    def create_default_service(s3fs_connection=None) -> S3DataService:
        """Create a default S3 data service."""
        return S3DataService(s3fs_connection)
    
    @staticmethod
    def create_service_with_logging(s3fs_connection=None, log_level=logging.INFO) -> S3DataService:
        """Create an S3 data service with custom logging."""
        logging.basicConfig(level=log_level)
        return S3DataService(s3fs_connection)