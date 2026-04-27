import lzma
import pytz
import pandas as pd
import numpy as np
import dill as pickle

def load_pickle(path):
    with lzma.open(path, "rb") as fp:
        file = pickle.load(fp)
    return file
    

def save_pickle(path, obj):
    with lzma.open(path, "wb") as fp:
        pickle.dump(obj, fp)


class Alpha():

    def __init__(self, insts, dfs, start, end):
        self.insts = insts
        self.dfs = dfs
        self.start = start
        self.end = end
    

    def init_portfolio_settings(self, trade_range):
        portfolio_df = pd.DataFrame(index=trade_range)\
            .reset_index()\
            .rename(columns={"index":"datetime"})
        portfolio_df.loc[0, "capital"] = 10000
        return portfolio_df


    def compute_meta_info(self, trade_range):
        for inst in self.insts:
            df = pd.DataFrame(index=trade_range)
            self.dfs[inst].index = self.dfs[inst].index.normalize()
            trade_range = trade_range.normalize()
            self.dfs[inst] =df.join(self.dfs[inst]).ffill().bfill()
            self.dfs[inst]["ret"] = -1 + self.dfs[inst]["close"]/self.dfs[inst]["close"].shift(1)
            sampled = self.dfs[inst]["close"] != self.dfs[inst]["close"].shift(1).bfill()
            eligible = sampled.rolling(5).apply(lambda x: int(np.any(x))).fillna(0)
            self.dfs[inst]["eligible"] = eligible.astype(int) & (self.dfs[inst]["close"] > 0).astype(int)
        
        return



    def run_simulation(self):
        print("running backtest")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        self.compute_meta_info(trade_range=date_range)
        portfolio_df = self.init_portfolio_settings(trade_range = date_range)
        for i in portfolio_df.index:
            date = portfolio_df.loc[i, "datetime"]

            eligibles = [inst for inst in self.insts if self.dfs[inst].loc[date, "eligible"]]
            non_eligibles = [inst for inst in self.insts if inst not in eligibles]

            if i != 0:
                #compute pnl
                pass

            day_data = self.dfs[self.insts[0]].loc[date]
            print(f"Date: {date} | Close: {day_data['close']} | Eligible: {day_data['eligible']}")

            alpha_scores = {}
            import random
            for inst in eligibles:
                alpha_scores[inst] = random.uniform(0, 1)
            input(alpha_scores)

            #compute positions and ...