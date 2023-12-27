import psycopg2
import pandas as pd
import time
from pathlib import Path
import os

from datetime import date, timedelta, datetime
from data.db_connection import DataConnection
from src.backtest import BacktestDerivatives
from src.pipeline import Pipeline
from utils.argument_management import Opts
from utils.loading_file import load_yaml


FILE = Path(__file__).resolve()
ROOT = FILE.parents[0]


def main():
    FLAGS = Opts().parse_args()
    db_account = load_yaml(ROOT / "configs" / "usr" / "db_account.yaml")
    # Data query
    user, password, host, port, db_name =   db_account['user'],\
                                            db_account['pass'], \
                                            db_account['host'], \
                                            db_account['port'], \
                                            db_account['database']
    db_conn = DataConnection(user, password, host, port, db_name)

    # Pipeline
    pipeline = Pipeline(FLAGS, db_conn)
    pipeline.backtest()

    
    
    

if __name__ == "__main__":
    main()
