import json
import pickle
from typing import Any, Dict

import joblib
import pandas as pd
from io_base import BaseIO, FileType, SourceInfo

import s3fs

# =============================================================================
# DOMAIN SERVICE - Business Logic That Doesn't Belong to Any Entity
# =============================================================================
def detect_file_type(path: str) -> FileType:
    """
    DOMAIN SERVICE: Encapsulates business logic for file type detection.
    This is domain knowledge about what file extensions map to what types.
    Domain services contain business logic that doesn't naturally fit into
    an entity or value object.
    """
    if path.endswith((".csv", ".parquet")):
        return FileType.PANDAS_DF
    elif path.endswith(".json"):
        return FileType.DICT
    elif path.endswith(".pickle"):
        return FileType.PICKLE
    elif path.endswith(".joblib"):
        return FileType.JOBLIB
    else:
        raise ValueError(f"Unsupported file type: {path}")


# =============================================================================
# INFRASTRUCTURE LAYER - Technical Implementation Details
# =============================================================================

class S3BaseIO(BaseIO):
    """
    INFRASTRUCTURE ADAPTER BASE CLASS:
    - Inherits from BaseIO (domain interface/contract)
    - Handles technical concern: S3 connection management
    - Bridge between domain interfaces and infrastructure implementation
    """
    def __init__(self, s3fs: s3fs.S3FileSystem = None):
        self.s3fs = s3fs  # INFRASTRUCTURE DEPENDENCY: S3 file system connection


class S3PandasDF(S3BaseIO):
    """
    CONCRETE INFRASTRUCTURE ADAPTER:
    - Implements BaseIO interface (follows interface contract)
    - Specializes in pandas DataFrame serialization
    - Technical implementation: Uses SageMaker's native S3 integration
    - Single Responsibility: Only handles pandas-compatible formats
    """
    
    def load(self, path: str, **kwargs):
        """CONCRETE IMPLEMENTATION: SageMaker-native pandas S3 loading"""
        if path.endswith(".csv"):
            return pd.read_csv(path, **kwargs)
        elif path.endswith(".parquet"):
            return pd.read_parquet(path, **kwargs)
        elif path.endswith(".json"):
            return pd.read_json(path, **kwargs)
        else:
            raise ValueError(f"Unsupported pandas format: {path}")

    def write(self, data: pd.DataFrame, path: str, **kwargs):
        """CONCRETE IMPLEMENTATION: SageMaker-native pandas S3 writing"""
        if path.endswith(".csv"):
            data.to_csv(path, index=False, **kwargs)
        elif path.endswith(".parquet"):
            data.to_parquet(path, index=False, **kwargs)
        elif path.endswith(".json"):
            data.to_json(path, orient="records", **kwargs)
        else:
            raise ValueError(f"Unsupported pandas format: {path}")


class S3Dict(S3BaseIO):
    """
    CONCRETE INFRASTRUCTURE ADAPTER:
    - Specializes in JSON/dictionary serialization
    - Technical implementation: Uses s3fs for file access
    - Single Responsibility: Only handles dictionary/JSON formats
    """
    
    def load(self, path: str, **kwargs):
        """CONCRETE IMPLEMENTATION: s3fs-based JSON loading"""
        with self.s3fs.open(path, "r") as f:
            return json.load(f)

    def write(self, data: Dict, path: str, **kwargs):
        """CONCRETE IMPLEMENTATION: s3fs-based JSON writing"""
        with self.s3fs.open(path, "w") as f:
            json.dump(data, f)
        return


class S3Pickle(S3BaseIO):
    """
    CONCRETE INFRASTRUCTURE ADAPTER:
    - Specializes in Python pickle serialization
    - Technical implementation: Uses s3fs for binary file access
    - Single Responsibility: Only handles pickle formats
    """
    
    def load(self, path: str, **kwargs):
        """CONCRETE IMPLEMENTATION: s3fs-based pickle loading"""
        with self.s3fs.open(path, "rb") as f:
            return pickle.load(f)

    def write(self, data: Any, path: str, **kwargs):
        """CONCRETE IMPLEMENTATION: s3fs-based pickle writing"""
        with self.s3fs.open(path, "wb") as f:
            pickle.dump(data, f)
        return


class S3Joblib(S3BaseIO):
    """
    CONCRETE INFRASTRUCTURE ADAPTER:
    - Specializes in joblib serialization (ML models)
    - Technical implementation: Uses s3fs for binary file access
    - Single Responsibility: Only handles joblib formats
    """
    
    def load(self, path: str, **kwargs):
        """CONCRETE IMPLEMENTATION: s3fs-based joblib loading"""
        with self.s3fs.open(path, "rb") as f:
            return joblib.load(f)

    def write(self, data: Any, path: str, **kwargs):
        """CONCRETE IMPLEMENTATION: s3fs-based joblib writing"""
        with self.s3fs.open(path, "wb") as f:
            joblib.dump(data, f)
        return


# =============================================================================
# APPLICATION LAYER - Orchestrates Domain and Infrastructure
# =============================================================================

class S3Repository:
    """
    APPLICATION SERVICE / REPOSITORY PATTERN:
    
    APPLICATION SERVICE because it:
    - Orchestrates domain services and infrastructure adapters
    - Provides simplified public API
    - Hides complexity from external consumers
    - Coordinates between domain logic and infrastructure
    
    REPOSITORY PATTERN because it:
    - Provides collection-like interface for data access
    - Abstracts away persistence details
    - Encapsulates data access logic
    - Follows repository contract
    """
    
    def __init__(self, s3_connection: s3fs.S3FileSystem = None):
        """
        DEPENDENCY INJECTION: Infrastructure dependency injected
        FACTORY PATTERN: Creates appropriate infrastructure adapters
        """
        # INFRASTRUCTURE DEPENDENCY MANAGEMENT
        self.s3fs = s3_connection or s3fs.S3FileSystem()

        # STRATEGY PATTERN: Different strategies for different file types
        # DEPENDENCY INJECTION: Injecting s3fs connection to each adapter
        self.sources = {
            FileType.PANDAS_DF: S3PandasDF(s3fs=self.s3fs),  # CONCRETE STRATEGY
            FileType.DICT: S3Dict(s3fs=self.s3fs),            # CONCRETE STRATEGY
            FileType.PICKLE: S3Pickle(s3fs=self.s3fs),        # CONCRETE STRATEGY
            FileType.JOBLIB: S3Joblib(s3fs=self.s3fs),        # CONCRETE STRATEGY
        }

    def _load_data(self, source_info: SourceInfo, **kwargs):
        """
        PRIVATE APPLICATION METHOD: Internal orchestration
        Uses STRATEGY PATTERN to delegate to appropriate infrastructure adapter
        """
        return self.sources[source_info.file_type].load(source_info.path, **kwargs)

    def _write_data(self, data: Any, source_info: SourceInfo, **kwargs):
        """
        PRIVATE APPLICATION METHOD: Internal orchestration
        Uses STRATEGY PATTERN to delegate to appropriate infrastructure adapter
        """
        return self.sources[source_info.file_type].write(
            data, source_info.path, **kwargs
        )

    def load(self, s3_path: str, aws_profile_name: str = None, **kwargs):
        """
        PUBLIC APPLICATION API: 
        - Facade pattern: Hides internal complexity
        - Orchestrates: Domain service + Infrastructure adapters
        - Translates: Simple parameters → Domain objects (SourceInfo)
        """
        # DOMAIN SERVICE USAGE: Business logic for file type detection
        file_type = detect_file_type(s3_path)

        # VALUE OBJECT CREATION: Creating SourceInfo with domain data
        source_info = SourceInfo(
            path=s3_path, aws_profile_name=aws_profile_name, file_type=file_type
        )
        
        # DELEGATION: to internal orchestration method
        return self._load_data(source_info, **kwargs)

    def write(self, data: Any, s3_path: str, aws_profile_name: str = None, **kwargs):
        """
        PUBLIC APPLICATION API: 
        - Facade pattern: Hides internal complexity
        - Orchestrates: Domain service + Infrastructure adapters
        - Translates: Simple parameters → Domain objects (SourceInfo)
        """
        # DOMAIN SERVICE USAGE: Business logic for file type detection
        file_type = detect_file_type(s3_path)
        
        # VALUE OBJECT CREATION: Creating SourceInfo with domain data
        source_info = SourceInfo(
            path=s3_path, aws_profile_name=aws_profile_name, file_type=file_type
        )
        
        # DELEGATION: to internal orchestration method
        return self._write_data(data, source_info, **kwargs)