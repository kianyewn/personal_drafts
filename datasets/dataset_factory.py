import logging
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import torch
from .training_dataset import TrainingDataset
from .validation_dataset import ValidationDataset
from .inference_dataset import InferenceDataset

logger = logging.getLogger(__name__)


class RecsysDataLoaderFactory:
    """Factory for creating appropriate data loaders per phase"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def create_training_loader(
        self,
        data: pd.DataFrame,
        batch_size: int,
        shuffle: bool = True,
        num_workers: int = 4,
    ) -> torch.utils.data.DataLoader:
        """Create training data loader"""
        dataset = TrainingDataset(
            data=data,
            feature_config=self.config["training_features"],
            timestamp=self.config.get("training_timestamp"),
            phase="training",
        )

        return torch.utils.data.DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=shuffle,
            num_workers=num_workers,
            collate_fn=self._collate_fn,
        )

    def create_validation_loader(
        self,
        data: pd.DataFrame,
        batch_size: int,
        shuffle: bool = False,
        num_workers: int = 2,
    ) -> torch.utils.data.DataLoader:
        """Create validation data loader"""
        dataset = ValidationDataset(
            data=data,
            feature_config=self.config["validation_features"],
            timestamp=self.config.get("validation_timestamp"),
            phase="validation",
        )

        return torch.utils.data.DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=shuffle,
            num_workers=num_workers,
            collate_fn=self._collate_fn,
        )

    def create_inference_loader(
        self,
        data: pd.DataFrame,
        batch_size: int,
        shuffle: bool = False,
        num_workers: int = 1,
    ) -> torch.utils.data.DataLoader:
        """Create inference data loader"""
        dataset = InferenceDataset(
            data=data,
            feature_config=self.config["inference_features"],
            timestamp=datetime.now(),  # Current time for inference
            phase="inference",
        )

        return torch.utils.data.DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=shuffle,
            num_workers=num_workers,
            collate_fn=self._collate_fn,
        )

    def _collate_fn(
        self, batch: List[Dict[str, torch.Tensor]]
    ) -> Dict[str, torch.Tensor]:
        """Custom collate function for batching"""
        if not batch:
            return {}

        result = {}
        for key in batch[0].keys():
            result[key] = torch.stack([item[key] for item in batch])

        return result       