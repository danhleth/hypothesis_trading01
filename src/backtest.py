from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Any
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import datetime, timedelta
from utils.loading_file import load_csv
import os
from pathlib import Path

def is_same_week_as_third_thursday(input_date):
    if type(input_date) is str:
        try:
            if "." in input_date:
                date_obj = datetime.strptime(input_date, "%Y-%m-%d %H:%M:%S.%f")
            elif ":" in input_date:
                date_obj = datetime.strptime(input_date, "%Y-%m-%d %H:%M:%S")
            else:
                date_obj = datetime.strptime(input_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Invalid date format. Please provide a date in the format '%Y-%m-%d' or '%Y-%m-%d %H:%M:%S.%f'.")
    else:
        date_obj = input_date
    # Find the first day of the month and the number of days in the month
    first_day_of_month = date_obj.replace(day=1)
    first_thursday = first_day_of_month + timedelta(days=((3-first_day_of_month.weekday()) % 7))
    third_thursday = first_thursday + timedelta(days=14)
    start_of_week = third_thursday - timedelta(days=third_thursday.weekday())
    
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week <= date_obj <= end_of_week


@dataclass
class InforIndicator:
    rewards_idx: np.ndarray =  np.array([])
    winrate: float = 0
    rrewards_idx: np.ndarray = np.array([])
    profits: np.ndarray = np.array([])
    monetary_metric:str = 'VND'


    def __init__(self, data:object, name:str=''):
        if type(data) is pd.DataFrame:
            self.df = data
        elif type(data) is str:
            self._load_from_csv(data)
        self.name = name

    def update_df(self, new_df):
        self.df = new_df
    
    def __str__(self):
        str_result = f"{self.name}:\n \
                    Total trades: {len(self.rewards_idx)}\n \
                    Winrate: {self.winrate}\n \
                    Avg rewards: {round(sum(self.rewards_idx)/len(self.rewards_idx),2)}\n \
                    Avg Relative rewards: {round(sum(self.rrewards_idx)/len(self.rrewards_idx),2)}\n \
                    Best Trade: {max(self.rewards_idx)}\n \
                    Worst Trade: {round(min(self.rewards_idx),2)}\n \
                    Total profits: {int(sum(self.profits))} {self.monetary_metric}\n"
        return str_result

    def cal_tax_fee(self, index_score, num_contracts):
        fee_at_vps = 2000*num_contracts if num_contracts < 200 else 1000*num_contracts
        fee_for_center = 2700 * num_contracts
        fee = (fee_at_vps + fee_for_center)
        tax = ((index_score * 100000.0 * num_contracts * 0.17)/2.0)*0.001
        return (fee+tax)

    def auto_analyze(self):
        start_price = self.df['start_price'].values
        end_price = self.df['end_price'].values
        self.rewards_idx = start_price - end_price
        self.rrewards_idx = self.rewards_idx / start_price
        # tax_fees = 47000
        tax_fees = self.cal_tax_fee(start_price, 1) + self.cal_tax_fee(end_price, 1)
        self.profits = (self.rewards_idx * 100000) - tax_fees
        wincount = sum(self.rewards_idx > 0)
        self.winrate = wincount / len(self.df)
    
    def save_chart(self, save_dir):
        accumulate = np.add.accumulate(self.profits)/100000.0
        fig = plt.figure()
        plt.plot([i for i in range(len(self.rewards_idx))], 
                 accumulate, 
                 color='black',)
        plt.xlabel('Date')
        plt.ylabel('Profit')
        plt.title(f'{self.name} Cumulative Change')
        filename = self.name.strip().replace(' ','')
        os.makedirs(Path(save_dir)/'chart', exist_ok=True)
        fig.savefig(f"{save_dir}/chart/{filename}.png")


@dataclass
class BacktestDerivatives:
    start_date: str
    end_date: str
    start_time: str
    end_time: str
    rewards_idx: np.ndarray =  np.array([])
    winrate: float = 0
    rrewards_idx: np.ndarray = np.array([])
    profits: np.ndarray = np.array([])
    df: pd.DataFrame = None
    maturity: bool = False
    def __init__(self, opts):
        self.opts = opts['opts']
        self.start_date = self.opts['time']['start_date']
        self.end_date = self.opts['time']['end_date']
        self.start_time = self.opts['time']['start_time']
        self.end_time = self.opts['time']['end_time']
        self.maturity = self.opts['maturity']
        

    def _report(self):
        print("Backtest report:")
        print("Start_date: ", self.start_date)
        print("End_date: ", self.end_date)
        print(self.analyzer)
        print(self.maturity_analyzer)
        print(self.nonmaturity_analyzer)
    
    def run_from_df(self, df):
        df['is_maturity'] = df['start_time'].apply(lambda x: is_same_week_as_third_thursday(x))

        self.analyzer = InforIndicator(self.opts['save_dir'],
                                       df, 
                                       name="Total Analysis")
        self.analyzer.auto_analyze()

        if self.maturity:
            maturity_df = self.df[self.df['is_maturity'] == True]
            self.maturity_analyzer = InforIndicator(self.opts['save_dir'],
                                                    maturity_df, 
                                                    name="Maturity Only")
            self.maturity_analyzer.auto_analyze()
            
            notmaturity_df = self.df[self.df['is_maturity']==False]
            self.nonmaturity_analyzer = InforIndicator(self.opts['save_dir'],
                                                data=notmaturity_df, 
                                                name="Except Maturity")
            self.nonmaturity_analyzer.auto_analyze()
        return vars(self)
        
    def run_from_csv(self, csv_file):
        df = load_csv(csv_file)
        return self.run_from_df(df)


    def save_visualize_chart(self, save_dir):
        '''
        Viualize the result of backtest
        '''
        self.analyzer.visualize()
        self.maturity_analyzer.visualize()
        self.nonmaturity_analyzer.visualize()