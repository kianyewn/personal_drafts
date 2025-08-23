import json
import pickle
from typing import Any

import joblib
import pandas as pd
from io_base import BaseIO, FileType, SourceInfo


def detect_file_type(path: str) -> FileType:
    if path.endswith(".csv"):
        return FileType.PANDAS_DF
    elif path.endswith(".json"):
        return FileType.DICT
    elif path.endswith(".pickle"):
        return FileType.PICKLE
    elif path.endswith(".joblib"):
        return FileType.JOBLIB
    else:
        raise ValueError(f"Unsupported file type: {path}")


class LocalJoblib(BaseIO):
    def load(self, path: str):
        return joblib.load(path)

    def write(self, data: Any, path: str):
        joblib.dump(data, path)


class LocalPandasDF(BaseIO):
    def load(self, path: str):
        return pd.read_csv(path)

    def write(self, data: pd.DataFrame, path: str):
        data.to_csv(path, index=False)


class LocalDict(BaseIO):
    def load(self, path: str):
        return json.load(path)

    def write(self, data: dict, path: str):
        json.dump(data, path)


class LocalPickle(BaseIO):
    def load(self, path: str):
        return pickle.load(path)

    def write(self, data: Any, path: str):
        pickle.dump(data, path)


class LocalRepository:
    def __init__(self):
        self.sources = {
            FileType.PANDAS_DF: LocalPandasDF(),
            FileType.DICT: LocalDict(),
            FileType.PICKLE: LocalPickle(),
            FileType.JOBLIB: LocalJoblib(),
        }

    def _load_data(self, source_info: SourceInfo, **kwargs):
        return self.sources[source_info.file_type].load(source_info.path, **kwargs)

    def _write_data(self, data: Any, source_info: SourceInfo, **kwargs):
        return self.sources[source_info.file_type].write(data, source_info.path, **kwargs)

    def load(self, path: str, **kwargs):
        file_type = detect_file_type(path)
        source_info = SourceInfo(path, file_type)
        return self._load_data(source_info, **kwargs)

    def write(self, data: Any, path: str, **kwargs):
        file_type = detect_file_type(path)
        source_info = SourceInfo(path, file_type)
        return self._write_data(data, source_info, **kwargs)
