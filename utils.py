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
    
    def run_simulation(self):
        print("running backtest")
        date_range = pd.date_range(start=self.start, end=self.end, freq="D")
        print(date_range)