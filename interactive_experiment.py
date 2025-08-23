# interactive_experiment.py
#!/usr/bin/env python3
"""
Interactive experiment script - run with: python -i interactive_experiment.py
"""
import numpy as np
import pandas as pd
from datasets.data_processing import DataProcessor
from test_logging import setup_logging

# Setup logging
# setup_logging(level="DEBUG", enable_console=True)
logger = setup_logging()

# Configuration
config = {
    "numerical_features": ["age", "price", "rating"],
    "categorical_features": ["user_id", "item_id", "category"],
}


# Create sample data
def create_sample_data():
    """Create sample data for experimentation"""
    np.random.seed(42)

    n_samples = 1000
    data = {
        "age": np.random.randint(18, 80, n_samples),
        "price": np.random.uniform(10, 100, n_samples),
        "rating": np.random.uniform(1, 5, n_samples),
        "user_id": [f"user_{i}" for i in range(n_samples)],
        "item_id": [f"item_{i}" for i in range(n_samples)],
    }

    return pd.DataFrame(data)



# Create sample data
def create_sample_data2():
    """Create sample data for experimentation"""
    np.random.seed(42)

    n_samples = 1000
    data = {
        "age": np.random.randint(18, 80, n_samples),
        "price": np.random.uniform(10, 100, n_samples),
        "rating": np.random.uniform(1, 5, n_samples),
        "user_id": [f"user_{i}" for i in range(n_samples)],
        "item_id": [f"item_{i}" for i in range(n_samples)],
    }

    return pd.DataFrame(data)


# Create processor
dp = DataProcessor(config)

# Create sample data
train_df = create_sample_data()

print("🚀 Interactive experiment ready!")
print("Available variables:")
print("  - dp: DataProcessor instance")
print("  - config: Configuration dictionary")
print("  - train_df: Training dataframe")
print("  - create_sample_data(): Function to create more data")
print("\nTry these commands:")
print("  - dp.fit(train_df)")
print("  - dp.get_encoder_info()")
print("  - help(dp)")

# Keep interactive session alive
if __name__ == "__main__":
    print("\n�� Tip: Use Ctrl+D to exit or 'exit()' to quit")
