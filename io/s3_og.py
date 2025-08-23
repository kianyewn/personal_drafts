import json
import pickle
from typing import Any, Dict

import joblib
import pandas as pd
from io_base import BaseIO, FileType, SourceInfo


import s3fs
def s3fs():
    return s3fs.S3FileSystem()

class S3BaseIO(BaseIO):
    """Imagine this is like a domain service, where you do not want to have to pass the s3fs connection to every single function"""
    def __init__(self, s3fs: s3fs.S3FileSystem = None):
        self.s3fs = s3fs # imagine you have connection to s3 here based on prod or dev

class S3PandasDF(S3BaseIO):
    """S3PandasDF, S3Dict, S3Pickle, and S3Joblib are NOT domains. They are infrastructure adapters or technical services."""
    def load(self, path: str, **kwargs):
        if path.endswith(".csv"):
            return pd.read_csv(f"{path}", **kwargs)
        elif path.endswith(".parquet"):
            return pd.read_parquet(f"{path}", **kwargs)
        elif path.endswith(".json"):
            return pd.read_json(f"{path}", **kwargs)
        else:
            raise ValueError(f"Unsupported file type: {path}")

    def write(self, data: pd.DataFrame, path: str, **kwargs):
        if path.endswith(".csv"):
            data.to_csv(f"{path}", index=False, **kwargs)
        elif path.endswith(".parquet"):
            data.to_parquet(f"{path}", index=False, **kwargs)
        elif path.endswith(".json"):
            data.to_json(f"{path}", orient="records")
        else:
            raise ValueError(f"Unsupported file type: {path}")


class S3Dict(S3BaseIO):
    def load(self, path: str, **kwargs):
        s3fs = self.s3fs.S3FileSystem()
        with s3fs.open(path, "rb") as f:
            return json.load(f)

    def write(self, data: Dict, path: str, **kwargs):
        s3fs = self.s3fs.S3FileSystem()
        with s3fs.open(path, "wb") as f:
            json.dump(data, f)
        return


class S3Pickle(S3BaseIO):
    def load(self, path: str, **kwargs):
        s3fs = self.s3fs.S3FileSystem()
        with s3fs.open(path, "rb") as f:
            return pickle.load(f)

    def write(self, data: Any, path: str, **kwargs):
        s3fs = self.s3fs.S3FileSystem()
        with s3fs.open(path, "wb") as f:
            pickle.dump(data, f)
        return


class S3Joblib(S3BaseIO):
    def load(self, path: str, **kwargs):
        s3fs = self.s3fs.S3FileSystem()
        with s3fs.open(path, "rb") as f:
            return joblib.load(f)

    def write(self, data: Any, path: str, **kwargs):
        s3fs = self.s3fs.S3FileSystem()
        with s3fs.open(path, "wb") as f:
            joblib.dump(data, f)
        return


class S3Repository:
    """Imagine this is like a repository, where you have a connection to s3 and you can load and write data to s3"""
    def __init__(self, s3fs: s3fs = None):
        self.s3fs = s3fs

        self.sources = {
            FileType.PANDAS_DF: S3PandasDF(s3fs=self.s3fs),
            FileType.DICT: S3Dict(s3fs=self.s3fs),
            FileType.PICKLE: S3Pickle(s3fs=self.s3fs),
            FileType.JOBLIB: S3Joblib(s3fs=self.s3fs),
        }

    def _load_data(self, source_info: SourceInfo, **kwargs):
        return self.sources[source_info.file_type].load(source_info.path, **kwargs)

    def _write_data(self, data: Any, source_info: SourceInfo, **kwargs):
        return self.sources[source_info.file_type].write(data, source_info.path, **kwargs)

    def load(self, s3_path: str, aws_profile_name: str = None, **kwargs):
        if s3_path.endswith(".csv"):
            file_type = FileType.PANDAS_DF
        elif s3_path.endswith(".json"):
            file_type = FileType.DICT
        elif s3_path.endswith(".pickle"):
            file_type = FileType.PICKLE
        elif s3_path.endswith(".joblib"):
            file_type = FileType.JOBLIB
        else:
            raise ValueError(f"Unsupported file type: {s3_path}")

        source_info = SourceInfo(
            path=s3_path, aws_profile_name=aws_profile_name, file_type=file_type
        )
        return self._load_data(source_info, **kwargs)

    def write(self, data: Any, s3_path: str, aws_profile_name: str = None, **kwargs):
        if s3_path.endswith(".csv"):
            file_type = FileType.PANDAS_DF
        elif s3_path.endswith(".json"):
            file_type = FileType.DICT
        elif s3_path.endswith(".pickle"):
            file_type = FileType.PICKLE
        elif s3_path.endswith(".joblib"):
            file_type = FileType.JOBLIB
        else:
            raise ValueError(f"Unsupported file type: {s3_path}")
        source_info = SourceInfo(
            path=s3_path, aws_profile_name=aws_profile_name, file_type=file_type
        )
        return self._write_data(data, source_info, **kwargs)
