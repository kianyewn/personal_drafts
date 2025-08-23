from io_base import SourceInfo, SourceType
from io.s3 import S3Source
from typing import Any
import pandas as pd


class DataLoader:
    def __init__(self):
        self.source_factory = {
            SourceType.S3: S3Source()
        }

    def load(self, input, **kwargs):
        """Smart loader for different sources"""
        if input.startswith("s3://"):
            return self.load_s3(input, **kwargs)
        elif input.startswith("local://"):
            return self.load_local(input, **kwargs)
        else:
            raise ValueError(f"Unsupported input: {input}")

    def load_data(self, source_info: SourceInfo, **kwargs):
        """Advanced method for complex configurations"""
        source = self.source_factory[source_info.source_type]
        return source._load_data(source_info, **kwargs)
    
    def write_data(self, data:Any, source_info: SourceInfo):
        """Advanced method for complex configurations"""
        source = self.source_factory[source_info.source_type]
        return source._write_data(data, source_info)
    
    def load_s3(self, s3_path,  aws_profile_name: str = None, **kwargs):
        return self.source_factory[SourceType.S3].load(s3_path, aws_profile_name, **kwargs)

    def write_s3(self, data:Any, s3_path:str, aws_profile_name: str = None, **kwargs):
        return self.source_factory[SourceType.S3].write(data, s3_path, aws_profile_name, **kwargs)

    def load_local(self, path: str, **kwargs):
        if path.endswith(".csv"):
            return pd.read_csv(f"{path}", **kwargs)
        elif path.endswith(".parquet"):
            return pd.read_parquet(f"{path}", **kwargs)
        elif path.endswith(".json"):
            return pd.read_json(f"{path}", **kwargs)
        return None

    def load_snowflake(self, source_info: SourceInfo):
        pass
    def load_spark(self, source_info: SourceInfo):
        pass
    def load_database(self, source_info: SourceInfo):
        pass
    def write_local(self, source_info: SourceInfo):
        pass
    def write_snowflake(self, source_info: SourceInfo):
        pass
    def write_spark(self, source_info: SourceInfo):
        pass
    def write_database(self, source_info: SourceInfo):
        pass