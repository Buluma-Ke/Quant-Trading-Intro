import lzma
import dill as pickle

def load_pickle(path):
    with lzma.open(path, "rb") as fp:
        file = pickle.load(fp)
    return file
    

def save_pickle(path, obj):
    with lzma.open(path, "wb") as fp:
        pickle.dump(obj, fp)


import pandas as pd
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
        print(portfolio_df)


    def run_simulation(self):
        print("running backtest")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        portfolio_df = self.init_portfolio_settings(trade_range = date_range)
        #print(date_range)