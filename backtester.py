
import pandas as pd

class Backtest():
    """
    Backtester base class

    Attributes
    ----------
    data: pandas.DataFrame
        Backtest data, remember to clean to ensure no gaps before using this backtester class.
        Dataframe index should be a multiindex of (timestamp, symbol).
        Columns should contain at least (open, high, low, close). Additional data columns may be supplied as required by the strategy.

        Notes for data source:
        If the strategy requires a lookback period of 10 bars the data should supply at least 10 bars before start_timestamp.
        Minimally, data should have at least 1 timestamp before start_timestamp. This is because the backtest is executed where
        on start_timestamp, it is only given the bar data of the timestamp prior and trades are executed at the open price of start_timestamp.

    Methods
    -------
    run(capital, start_timestamp, end_timestamp):
        Run the backtest. Refer to data source notes to ensure timestamps are appropriate for the supplied data. 
        
        capital: float
            Starting amount of money
        start_timestamp:
            Start of backtest. Should lie within the range available within the data. 
        end_timestamp:
            End of backtest. Should lie within the range available within the data.
    on_init():
        Method executed at the start of the backtest. May be used for performing pre-calculations, opening initial positions, etc.
        Should only be implemented by classes extending from the base class. 
    on_bar(bar, bar_ts):
        Method executed after every bar, usually the core logic of the strategy.
        Should only be implemented by classes extending from the base class. 

        bar_ts: pandas.Timestamp
            Timestamp of the incoming bar
        bar: pandas.DataFrame
            Same schema as data but without the timestamp index.
    trade(symbol, qty):
        Perform a trade of the symbol with the specified quantity.

        symbol: str
            Asset/instrument being traded
        qty: float
            Quantity of the trade. Negative quantity is taken to be a sell / short position. 
    """
    def __init__(self, data: pd.DataFrame):
        self.data = data
        self.timestamps = self.data.index.get_level_values(0).unique()

    def run(self, capital: float, start_timestamp, end_timestamp):
        backtest_period = self.timestamps.to_series()[start_timestamp:end_timestamp]
        start_idx = self.timestamps.get_loc(backtest_period[0]) - 1
        start_ts = self.timestamps[start_idx]
        end_idx = self.timestamps.get_loc(backtest_period[-1]) - 1
        
        self.cash = capital
        self.positions = {}
        self.ts_dict = {"timestamp": start_ts}
        self.position_history = [{**self.ts_dict}]
        self.cash_history = [{**self.ts_dict, "cash": self.cash}]
        self.trade_history = []

        for idx in range(start_idx, end_idx + 1):
            bar_ts = self.timestamps[idx]
            self.current_ts = self.timestamps[idx + 1]
            self.current_prices = self.data.loc[self.current_ts].open
            if idx == start_idx:
                self.on_init()
            self.on_bar(self.data.loc[bar_ts], bar_ts)

            self.ts_dict = {"timestamp": self.current_ts}
            self.position_history.append({**self.ts_dict, **self.positions})
            self.cash_history.append({**self.ts_dict, "cash": self.cash})
        
        self.position_history = pd.DataFrame(self.position_history).set_index("timestamp").fillna(0)
        self.cash_history = pd.DataFrame(self.cash_history).set_index("timestamp")
        self.trade_history = pd.DataFrame(self.trade_history)

        close_prices = self.data.close.loc[(slice(backtest_period[0], backtest_period[-1]), self.position_history.keys())].unstack()
        self.close_prices = close_prices
        self.portfolio_value_history = pd.concat([self.position_history * close_prices, self.cash_history], axis=1).sum(axis=1)
    
    def calculate_statistics(self):
        pass

    def trade(self, symbol: str, qty: float):
        if symbol in self.positions:
            self.positions[symbol] += qty
        else:
            self.positions[symbol] = qty
        self.cash -= qty * self.current_prices.loc[symbol]
        self.trade_history.append({**self.ts_dict, "symbol": symbol, "qty": qty})

    def on_init(self):
        pass

    def on_bar(self, bar: pd.DataFrame, bar_ts: pd.DataFrame):
        pass