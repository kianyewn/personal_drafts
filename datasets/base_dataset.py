import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import torch

logger = logging.getLogger(__name__)


class BaseDataset(torch.utils.data.Dataset, ABC):
    """Base class for recommendation system datasets"""

    def __init__(
        self,
        data: pd.DataFrame,
        feature_config: Dict[str, Any],
        timestamp: Optional[datetime] = None,
        phase: str = "unknown",
    ):
        super().__init__()
        self.data = data
        self.feature_config = feature_config
        self.timestamp = timestamp or datetime.now()
        self.phase = phase

        # Validate data schema
        self._validate_data()

        # Process features based on phase
        self.processed_data = self._process_features()

        logger.info(
            f"Initialized {self.__class__.__name__} for {phase} phase",
            data_size=len(data),
            feature_count=len(self.feature_config),
            timestamp=self.timestamp.isoformat(),
        )

    @abstractmethod
    def _process_features(self) -> pd.DataFrame:
        """Process features based on phase-specific requirements"""
        pass

    def _validate_data(self):
        """Validate data schema and required columns"""
        required_cols = self.feature_config.get("required_columns", [])
        missing_cols = set(required_cols) - set(self.data.columns)

        if missing_cols:
            raise ValueError(
                f"Missing required columns for {self.phase} phase: {missing_cols}"
            )

    def __len__(self) -> int:
        return len(self.processed_data)

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        row = self.processed_data.iloc[idx]
        return self._format_row(row)

    def _format_row(self, row: pd.Series) -> Dict[str, torch.Tensor]:
        """Convert pandas row to tensor format"""
        result = {}

        for col, dtype in self.feature_config.get("dtypes", {}).items():
            if col in row:
                if dtype == "float":
                    result[col] = torch.tensor(row[col], dtype=torch.float32)
                elif dtype == "long":
                    result[col] = torch.tensor(row[col], dtype=torch.long)
                elif dtype == "bool":
                    result[col] = torch.tensor(row[col], dtype=torch.bool)

        return result
