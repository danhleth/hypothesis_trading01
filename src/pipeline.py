from data.db_connection import DataConnection
from src.backtest import BacktestDerivatives
import os


class Pipeline():
    def __init__(self, opts,  
                 db_conn:DataConnection):
        self.opts = opts
        self.df = db_conn.get_derivative_matched_data(self.opts['opts']['time']['start_date'],
                                                self.opts['opts']['time']['end_date'],
                                                self.opts['opts']['time']['start_time'],
                                                self.opts['opts']['time']['end_time'],
                                                self.opts['opts']['time']['second_delay'])
        
    def fit(self):
        pass
    def backtest(self):
        backtest_service = BacktestDerivatives(self.opts)
        
        rs = backtest_service.run_from_df(self.df)
        backtest_service.save_visualize_chart(self.opts['opts']['save_dir'])