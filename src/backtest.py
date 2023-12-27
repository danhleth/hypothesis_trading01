from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from datetime import datetime, timedelta
import calendar


def is_same_week_as_third_thursday(input_date):
    # Convert input date string to datetime object
    try:
        date_obj = datetime.strptime(input_date, "%Y-%m-%d")
    except ValueError:
        try:
            date_obj = datetime.strptime(input_date, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            raise ValueError("Invalid date format. Please provide a date in the format '%Y-%m-%d' or '%Y-%m-%d %H:%M:%S.%f'.")

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
    
    def _load_from_csv(self, csv_file):
        '''
            csv_file: path to dataset file
            the structure format as follow:
            start_time, start_price, end_time, end_price
        '''
        self.df = pd.read_csv(csv_file)

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
    
    def visualize(self):
        accumulate = np.add.accumulate(self.profits)/100000.0
        fig = plt.figure()
        plt.plot([i for i in range(len(self.rewards_idx))], 
                 accumulate, 
                 color='black',)
        plt.xlabel('Date')
        plt.ylabel('Profit')
        plt.title(f'{self.name} Cumulative Change')
        filename = self.name.strip().replace(' ','')
        fig.savefig(f"{filename}.png")



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
    def _load_from_csv(self, csv_file):
        '''
            csv_file: path to dataset file
            the structure format as follow:
            start_time, start_price, end_time, end_price
        '''
        self.df = pd.read_csv(csv_file)

    def _report(self):
        print("Backtest report:")
        print("Start_date: ", self.start_date)
        print("End_date: ", self.end_date)
        print(self.analyzer)
        print(self.maturity_analyzer)
        print(self.nonmaturity_analyzer)


    def run_from_csv(self, csv_file, report=False):
        self._load_from_csv(csv_file)
        self.df['is_maturity'] = self.df['start_time'].apply(lambda x: is_same_week_as_third_thursday(x))
        self.df.to_csv("bruh.csv", index=False)
        self.analyzer = InforIndicator(self.df, 
                                       name="Total Analysis")
        self.analyzer.auto_analyze()

        maturity_df = self.df[self.df['is_maturity'] == True]
        self.maturity_analyzer = InforIndicator(maturity_df, 
                                                name="Maturity Only")
        self.maturity_analyzer.auto_analyze()
        
        notmaturity_df = self.df[self.df['is_maturity']==False]
        self.nonmaturity_analyzer = InforIndicator(data=notmaturity_df, 
                                             name="Except Maturity")
        self.nonmaturity_analyzer.auto_analyze()

        if report:
            self._report()

    def visualize(self):
        '''
        Viualize the result of backtest
        '''
        self.analyzer.visualize()
        self.maturity_analyzer.visualize()
        self.nonmaturity_analyzer.visualize()