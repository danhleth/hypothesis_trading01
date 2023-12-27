import yaml
import pandas as pd


def load_yaml(path):
    with open(path, 'rt') as f:
        return yaml.safe_load(f)
    
def load_csv(path):
    return pd.read_csv(path)