import psycopg2
import pandas as pd
import time
from pathlib import Path
import os

from datetime import date, timedelta, datetime
from data.db_connection import DataConnection
from src.backtest import BacktestDerivatives
from utils.path_management import increment_path

db_name = "algotradeDB"
host = "algotrade.vn"
user = ""
port = 52
password = "h"

FILE = Path(__file__).resolve()
ROOT = FILE.parents[0]


def main():

    # Data query
    db_conn = DataConnection(user, password, host, port, db_name)

    df = db_conn.get_derivative_matched_data('2022-09-01', 
                  '2023-12-01',
                  '10:30:00',
                  '11:29:45', 
                  10)
    df.to_csv("dataset/data_timedelay10_before112945.csv", index=False)


    backtest = BacktestDerivatives('2022-09-01', 
                  '2023-12-01',
                  '10:30:00',
                  '11:29:45')
    
    save_dir = increment_path(Path(ROOT) / 'exp', exist_ok=True)  # increment run
    backtest.run_from_csv("dataset/data_timedelay10_after113045.csv", report=True)
    backtest.visualize()

if __name__ == "__main__":
    main()
