import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

import s3fs
from io_base import BaseIO, FileType

logger = logging.getLogger(__name__)

# Impact Analysis:

# DOMAIN LAYER - Likely needs updates
# - SourceInfo value object may need new fields
# - New domain concepts (encryption, access patterns)

# INFRASTRUCTURE LAYER - Definitely needs updates
# - S3 connection creation logic
# - Individual adapters might need new config

# APPLICATION LAYER - Minimal changes
# - S3Repository might need new constructor parameters
# - Public API should remain mostly the same (backward compatibility)

# io_base.py - Enhance domain concepts


class AccessPattern(Enum):
    """New domain concept: How can we access this data?"""

    READ_ONLY = "read_only"
    WRITE_ONLY = "write_only"
    FULL_ACCESS = "full_access"


class EncryptionType(Enum):
    """New domain concept: How should data be encrypted?"""

    NONE = "none"
    SSE_S3 = "sse_s3"
    SSE_KMS = "sse_kms"


@dataclass
class S3Config:
    """New domain value object: Company S3 configuration"""

    aws_profile: Optional[str] = None
    endpoint_url: Optional[str] = None  # Custom endpoint
    region: Optional[str] = None
    encryption_type: EncryptionType = EncryptionType.SSE_S3
    kms_key_id: Optional[str] = None
    access_pattern: AccessPattern = AccessPattern.FULL_ACCESS
    max_retries: int = 3
    timeout_seconds: int = 60


@dataclass
class SourceInfo:
    """Enhanced to include new company requirements"""

    path: str
    file_type: FileType
    s3_config: Optional[S3Config] = None  # New field

    # Keep backward compatibility
    aws_profile_name: Optional[str] = None  # Deprecated but supported

    def __post_init__(self):
        # Migration logic for backward compatibility
        if self.aws_profile_name and not self.s3_config:
            self.s3_config = S3Config(aws_profile=self.aws_profile_name)


# s3.py - Update infrastructure adapters


class S3ConnectionFactory:
    """New infrastructure service: Creates configured S3 connections"""

    @staticmethod
    def create_connection(config: S3Config) -> s3fs.S3FileSystem:
        """Factory method to create properly configured S3 connection"""
        connection_kwargs = {}

        if config.aws_profile:
            connection_kwargs["profile"] = config.aws_profile

        if config.endpoint_url:
            connection_kwargs["endpoint_url"] = config.endpoint_url

        if config.region:
            connection_kwargs["default_region"] = config.region

        # Add retry configuration
        connection_kwargs["config"] = {
            "retries": {"max_attempts": config.max_retries},
            "read_timeout": config.timeout_seconds,
        }

        return s3fs.S3FileSystem(**connection_kwargs)


# Similar enhancements for S3PandasDF, S3Pickle, S3Joblib...
class S3BaseIO(BaseIO):
    """Accept either connection or config - let caller decide
    Enhanced base class with configuration support
        Inject the connection - don't create it!
    """

    def __init__(
        self, s3_connection: s3fs.S3FileSystem = None, s3_config: S3Config = None
    ):
        if s3_connection:
            # Direct injection (best for testing and performance)
            self.s3fs = s3_connection
            self.s3_config = s3_config or S3Config()
        else:
            # Lazy creation (convenience for simple usage)
            self.s3_config = s3_config or S3Config()
            self._s3fs = None

    @property
    def s3fs(self) -> s3fs.S3FileSystem:
        if hasattr(self, "_s3fs") and self._s3fs is None:
            self._s3fs = S3ConnectionFactory.create_connection(self.s3_config)
        return self._s3fs

    def _get_write_kwargs(self, **kwargs):
        """Helper to add encryption settings to write operations"""
        write_kwargs = kwargs.copy()

        if self.s3_config.encryption_type == EncryptionType.SSE_KMS:
            write_kwargs["ServerSideEncryption"] = "aws:kms"
            if self.s3_config.kms_key_id:
                write_kwargs["SSEKMSKeyId"] = self.s3_config.kms_key_id
        elif self.s3_config.encryption_type == EncryptionType.SSE_S3:
            write_kwargs["ServerSideEncryption"] = "AES256"

        return write_kwargs

    def _check_access_permission(self, operation: str):
        """Domain logic: Check if operation is allowed"""
        if (
            operation == "read"
            and self.s3_config.access_pattern == AccessPattern.WRITE_ONLY
        ):
            raise PermissionError(
                "Read operation not allowed with WRITE_ONLY access pattern"
            )
        elif (
            operation == "write"
            and self.s3_config.access_pattern == AccessPattern.READ_ONLY
        ):
            raise PermissionError(
                "Write operation not allowed with READ_ONLY access pattern"
            )


class S3Dict(S3BaseIO):
    """Enhanced with new configuration support"""

    def load(self, path: str, **kwargs):
        """Enhanced with access control and retry logic"""
        self._check_access_permission("read")  # New: Access control

        try:
            with self.s3fs.open(path, "r") as f:
                return json.load(f)
        except Exception as e:
            # Enhanced error handling with company requirements
            logger.error(f"Failed to load {path} with config {self.s3_config}: {e}")
            raise

    def write(self, data: Dict, path: str, **kwargs):
        """Enhanced with encryption and access control"""
        self._check_access_permission("write")  # New: Access control

        # Apply company encryption requirements
        write_kwargs = self._get_write_kwargs(**kwargs)

        try:
            with self.s3fs.open(path, "w", **write_kwargs) as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Failed to write {path} with config {self.s3_config}: {e}")
            raise


# Similar enhancements for S3PandasDF, S3Pickle, S3Joblib...
class S3BaseIO(BaseIO):
    def __init__(self, s3_connection: s3fs.S3FileSystem):
        """Inject the connection - don't create it!"""
        self.s3fs = s3_connection  # ✅ GOOD!


class S3Repository:
    def __init__(self, s3_config: S3Config = None):
        """Repository creates connection once and injects it"""
        self.s3_config = s3_config or S3Config()

        # Create connection ONCE at repository level
        self.s3fs = S3ConnectionFactory.create_connection(self.s3_config)

        # Inject same connection to all adapters
        self.sources = {
            FileType.DICT: S3Dict(s3_connection=self.s3fs),  # ✅ Inject
        }
