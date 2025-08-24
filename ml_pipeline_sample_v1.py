import joblib
from typing import Optional
from sklearn.model_selection import train_test_split
import pickle
import pandas as pd

# Infrastruture
class DataLoader:
    def __init__(self, config):
        self.config = config
        

class Demographics:
    """Domain service"""

    def __init__(self, data_loader, config, is_sample=False):
        self.data_loader = data_loader
        self.config = config
        self.is_sample = is_sample

    def sample_data(self, demographics):
        """All the logic that samples the data"""
        demographics = demographics.sample(frac=0.1, random_state=42)
        return demographics

    def clean_data(self, demographics):
        """All the logic that cleans the data"""
        pass

    def filter_data(self, demographics):
        """All the logic that removes data from the dataset"""
        pass

    def feature_engineering(self, demographics):
        """All the logic that creates features"""
        pass

    def process_data(self, demographics):
        """All the logic that processes the data"""
        demographics = self.clean_data(demographics)
        demographics = self.filter_data(demographics)
        demographics = self.feature_engineering(demographics)
        return demographics

    def load_demographic_df(self, demographics):
        if self.is_sample:
            demographics = self.sample_data(demographics)
        demographics_df = self.process_data(demographics)
        return demographics_df


class Transactions:
    # Domain Service
    def __init__(self, data_loader, config, is_sample=False):
        self.data_loader = data_loader
        self.config = config
        self.is_sample = is_sample

    def sample_data(self, transactions):
        """All the logic that samples the data"""
        transactions = transactions.sample(frac=0.1, random_state=42)
        return transactions

    def clean_data(self, transactions):
        """All the logic that cleans the data"""
        pass

    def filter_data(self, transactions):
        """All the logic that removes data from the dataset"""
        pass

    def feature_engineering(self, transactions):
        """All the logic that creates features"""
        pass

    def process_data(self, transactions):
        """All the logic that processes the data"""
        transactions = self.clean_data(transactions)
        transactions = self.filter_data(transactions)
        transactions = self.feature_engineering(transactions)
        return transactions

    def load_transactions_df(self, transactions):
        if self.is_sample:
            transactions = self.sample_data(transactions)
        transactions_df = self.process_data(transactions)
        return transactions_df


class CombineFeatures:
    # Domain Service
    def __init__(
        self,
        config,
        demographics_df,
        transactions_df,
        numerical_encoder,
        categorical_encoder,
    ):
        self.config = config
        self.data = {
            "demographics_df": demographics_df,
            "transactions_df": transactions_df,
        }
        self.numerical_encoder = numerical_encoder
        self.categorical_encoder = categorical_encoder

    def combine_data(self):
        master_df = pd.merge(
            self.data["demographics_df"],
            self.data["transactions_df"],
            on="customer_id",
            how="left",
        )
        return master_df

    def train_test_split(self, master_df):
        """Split the data into train and test sets"""
        train_val_df, test_df = train_test_split(
            master_df, test_size=0.2, random_state=42
        )
        train_df, val_df = train_test_split(
            train_val_df, test_size=0.2, random_state=42
        )
        return train_df, val_df, test_df

    def feature_engineering_before_split(
        self, df, reference_date: Optional[str] = None
    ):
        """Feature Engineering after merging, but before split. Cross features are created here."""
        pass

    def feature_engineering_after_split(self, df, training: bool = True):
        """Feature Engineering after split. Time series features are created here.
        Feature engineering includes:
        - Time series features
        - Cross features
        - Target features
        - Feature selection
        - Feature scaling
        - Feature encoding
        - Feature imputation
        """
        if training:
            self.fit_encoders(df)
            return self.transform_encoders(df)
        else:
            return self.transform_encoders(df)

    def fit_encoders(self, train_df):
        self.numerical_encoder.fit(train_df)
        self.categorical_encoder.fit(train_df)
        return self

    def transform_encoders(self, df):
        df = self.numerical_encoder.transform(df)
        df = self.categorical_encoder.transform(df)
        return df

    def combine_features_for_training(self):
        master_df = self.combine_data()
        master_df = self.feature_engineering_before_split(master_df)
        train_df, val_df, test_df = self.train_test_split(master_df)
        train_df = self.feature_engineering_after_split(train_df, training=True)
        val_df = self.feature_engineering_after_split(val_df, training=False)
        test_df = self.feature_engineering_after_split(test_df, training=False)
        processed_master_df = self.feature_engineering_after_split(
            master_df, training=False
        )
        return train_df, val_df, test_df, processed_master_df

    def combine_features_for_inference(self):
        master_df = self.combine_data()
        master_df = self.feature_engineering_before_split(master_df)
        self.load(self.config["encoder_path"])
        processed_master_df = self.feature_engineering_after_split(
            master_df, training=False
        )
        return master_df, processed_master_df

    def save(self):
        state = {
            "numerical_encoder": self.numerical_encoder,
            "categorical_encoder": self.categorical_encoder,
        }
        with open("state.pkl", "wb") as f:
            pickle.dump(state, f)

    def load(self):
        with open("state.pkl", "rb") as f:
            state = pickle.load(f)
        self.numerical_encoder = state["numerical_encoder"]
        self.categorical_encoder = state["categorical_encoder"]
        return self


class TrainingPipeline:
    # Application Service
    def __init__(self, data_loader=None, config=None, model=None):
        self.data_loader = data_loader if data_loader is not None else DataLoader()
        self.config = config
        self.model = model if model is not None else None

    def load_data(self):
        demographics = Demographics(
            self.data_loader, self.config, is_sample=True
        ).load_demographic_df()
        transactions = Transactions(
            self.data_loader, self.config, is_sample=True
        ).load_transactions_df()
        train_df, val_df, test_df, processed_master_df = CombineFeatures(
            self.config, demographics, transactions
        ).combine_features_for_training()
        return {
            "demographics": demographics,
            "transactions": transactions,
            "train_df": train_df,
            "val_df": val_df,
            "test_df": test_df,
            "processed_master_df": processed_master_df,
        }

    def train(self):
        data = self.load_data()
        # tune data accoring to training and validation
        self.model.tune(data["train_df"], data["val_df"])
        self.model.fit(data["train_df"])

        # final evaluation on test set
        self.model.evaluate(data["test_df"])
        self.model.save()


class InferencePipeline:
    # Application Service
    def __init__(self, data_loader, config):
        self.data_loader = data_loader
        self.config = config

    def load_model(self):
        return joblib.load(self.config["model_path"])

    def load_data(self):
        demographics = Demographics(
            self.data_loader, self.config, is_sample=False
        ).load_demographic_df()
        transactions = Transactions(
            self.data_loader, self.config, is_sample=False
        ).load_transactions_df()
        master_df, processed_master_df = CombineFeatures(
            self.config, demographics, transactions
        ).combine_features_for_inference()
        return {"processed_master_df": processed_master_df}

    def inference(self):
        data = self.load_data()
        predictions = self.model.predict(data)
        return predictions
