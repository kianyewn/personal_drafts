import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler

logger = logging.getLogger(__name__)


class NumericalEncoder:
    """Numerical feature encoder"""

    def __init__(self, numerical_features: List[str]):
        self.numerical_features = numerical_features
        self.scaler = StandardScaler()
        self.is_fitted = False

    def fit(self, df: pd.DataFrame) -> "NumericalEncoder":
        if not self.numerical_features:
            return self

        missing_features = set(self.numerical_features) - set(df.columns)
        if missing_features:
            raise ValueError(f"Missing numerical features: {missing_features}")

        feature_data = df[self.numerical_features].fillna(0)
        self.scaler.fit(feature_data)
        self.is_fitted = True

        logger.info(f"Fitted numerical encoder for features: {self.numerical_features}")
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.is_fitted:
            raise RuntimeError("Encoder must be fitted before transform")

        if not self.numerical_features:
            return df

        missing_features = set(self.numerical_features) - set(df.columns)
        if missing_features:
            raise ValueError(f"Missing numerical features: {missing_features}")

        feature_data = df[self.numerical_features].fillna(0)
        transformed_data = self.scaler.transform(feature_data)

        result = df.copy()
        for i, feature in enumerate(self.numerical_features):
            result[f"{feature}_scaled"] = transformed_data[:, i]

        return result


class CategoricalEncoder:
    """Categorical feature encoder"""

    def __init__(self, categorical_features: List[str]):
        self.categorical_features = categorical_features
        self.encoders = {}
        self.is_fitted = False

    def fit(self, df: pd.DataFrame) -> "CategoricalEncoder":
        if not self.categorical_features:
            return self

        missing_features = set(self.categorical_features) - set(df.columns)
        if missing_features:
            raise ValueError(f"Missing categorical features: {missing_features}")

        for feature in self.categorical_features:
            encoder = LabelEncoder()
            encoder.fit(df[feature].fillna("unknown"))
            self.encoders[feature] = encoder

        self.is_fitted = True
        logger.info(
            f"Fitted categorical encoder for features: {self.categorical_features}"
        )
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.is_fitted:
            raise RuntimeError("Encoder must be fitted before transform")

        if not self.categorical_features:
            return df

        missing_features = set(self.categorical_features) - set(df.columns)
        if missing_features:
            raise ValueError(f"Missing categorical features: {missing_features}")

        result = df.copy()
        for feature in self.categorical_features:
            if feature in self.encoders:
                encoder = self.encoders[feature]
                result[f"{feature}_encoded"] = encoder.transform(
                    df[feature].fillna("unknown")
                )

        return result


class DataProcessor:
    """Smart data processor with automatic encoder creation"""

    def __init__(self, config: Dict, **kwargs):
        self.config = config

        # Smart defaults: Create encoders automatically based on config
        self.numerical_encoder = (
            kwargs.get("numerical_encoder") or self._create_numerical_encoder()
        )
        self.categorical_encoder = (
            kwargs.get("categorical_encoder") or self._create_categorical_encoder()
        )

        # Future-proof: Easy to add new encoder types
        self.text_encoder = kwargs.get("text_encoder") or self._create_text_encoder()
        self.temporal_encoder = (
            kwargs.get("temporal_encoder") or self._create_temporal_encoder()
        )

        self.is_initialized = False

        logger.info(f"Initialized DataProcessor with config: {list(config.keys())}")

    def _create_numerical_encoder(self) -> NumericalEncoder:
        """Automatically create numerical encoder based on config"""
        numerical_features = self.config.get("numerical_features", [])
        return NumericalEncoder(numerical_features)

    def _create_categorical_encoder(self) -> CategoricalEncoder:
        """Automatically create categorical encoder based on config"""
        categorical_features = self.config.get("categorical_features", [])
        return CategoricalEncoder(categorical_features)

    def _create_text_encoder(self):
        """Placeholder for future text encoder"""
        text_features = self.config.get("text_features", [])
        if text_features:
            logger.info(
                f"Text features detected: {text_features} (encoder not implemented yet)"
            )
        return None

    def _create_temporal_encoder(self):
        """Placeholder for future temporal encoder"""
        temporal_features = self.config.get("temporal_features", [])
        if temporal_features:
            logger.info(
                f"Temporal features detected: {temporal_features} (encoder not implemented yet)"
            )
        return None

    # Dependency injection still available for advanced users
    def set_numerical_encoder(self, encoder: NumericalEncoder):
        """Override numerical encoder (useful for testing or customization)"""
        self.numerical_encoder = encoder
        self.is_initialized = False
        logger.info("Numerical encoder overridden")

    def set_categorical_encoder(self, encoder: CategoricalEncoder):
        """Override categorical encoder (useful for testing or customization)"""
        self.categorical_encoder = encoder
        self.is_initialized = False
        logger.info("Categorical encoder overridden")

    def fit_encoders(self, df: pd.DataFrame) -> "DataProcessor":
        """Fit all encoders on training data"""
        logger.info("Fitting encoders on training data...")

        # Fit numerical encoder
        if self.numerical_encoder and self.numerical_encoder.numerical_features:
            self.numerical_encoder.fit(df)

        # Fit categorical encoder
        if self.categorical_encoder and self.categorical_encoder.categorical_features:
            self.categorical_encoder.fit(df)

        # Future: Fit other encoders
        # if self.text_encoder: ...
        # if self.temporal_encoder: ...

        self.is_initialized = True
        logger.info("All encoders fitted successfully")
        return self

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process data using fitted encoders"""
        if not self.is_initialized:
            raise RuntimeError(
                "Encoders must be fitted before processing. Call fit_encoders() first."
            )

        result = df.copy()

        # Process numerical features
        if self.numerical_encoder and self.numerical_encoder.is_fitted:
            result = self.numerical_encoder.transform(result)

        # Process categorical features
        if self.categorical_encoder and self.categorical_encoder.is_fitted:
            result = self.categorical_encoder.transform(result)

        # Future: Process other features
        # if self.text_encoder: ...
        # if self.temporal_encoder: ...

        logger.info(f"Processed data: {df.shape} -> {result.shape}")
        return result

    def save(self, path: str) -> None:
        """Save entire processor state in one file"""
        state = {
            "config": self.config,
            "is_initialized": self.is_initialized,
            "numerical_encoder": self.numerical_encoder,
            "categorical_encoder": self.categorical_encoder,
            "version": "1.0",
            "timestamp": datetime.now().isoformat(),
        }

        joblib.dump(state, path)
        logger.info(f"Saved complete processor state to: {path}")

    def load(self, path: str) -> "DataProcessor":
        """Load complete processor state from one file"""
        if not Path(path).exists():
            raise FileNotFoundError(f"Processor file not found: {path}")

        try:
            state = joblib.load(path)

            # Restore state
            self.config = state["config"]
            self.is_initialized = state["is_initialized"]
            self.numerical_encoder = state["numerical_encoder"]
            self.categorical_encoder = state["categorical_encoder"]

            logger.info(f"Loaded complete processor state from: {path}")
            return self

        except Exception as e:
            raise ValueError(f"Failed to load processor from {path}: {e}")

    @classmethod
    def from_file(cls, path: str) -> "DataProcessor":
        """Create processor instance from saved file"""
        if not Path(path).exists():
            raise FileNotFoundError(f"Processor file not found: {path}")

        try:
            state = joblib.load(path)
            processor = cls(state["config"])

            # Restore state
            processor.is_initialized = state["is_initialized"]
            processor.numerical_encoder = state["numerical_encoder"]
            processor.categorical_encoder = state["categorical_encoder"]

            return processor

        except Exception as e:
            raise ValueError(f"Failed to create processor from {path}: {e}")

    def get_encoder_info(self) -> Dict[str, Any]:
        """Get information about fitted encoders"""
        info = {
            "is_initialized": self.is_initialized,
            "numerical_encoder": None,
            "categorical_encoder": None,
        }

        if self.numerical_encoder:
            info["numerical_encoder"] = {
                "features": self.numerical_encoder.numerical_features,
                "is_fitted": self.numerical_encoder.is_fitted,
            }

        if self.categorical_encoder:
            info["categorical_encoder"] = {
                "features": self.categorical_encoder.categorical_features,
                "is_fitted": self.categorical_encoder.is_fitted,
            }

        return info
