from configparser import ConfigParser
import ccxt
import time
import pandas as pd
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from collections import defaultdict
import schedule
import os
import logging

class DCA:
    def __init__(self, config):
        self.cfg = ConfigParser()
        self.cfg.optionxform = str
        self.cfg.read(config)
        # self.base_url = self.cfg['account']['base_url']
        self.minium_cost = float(self.cfg['general']['minium_cost'])
        self.total_cost = float(self.cfg['general']['total_cost'])
        self.symbols_ratio = self.cfg['symbols_ratio']
        self.currency = self.cfg['general']['currency']
        self.total_ratio = sum(map(float, self.symbols_ratio.values()))
        self.exchange = ccxt.binance({
            'apiKey': os.environ.get("API_KEY"),
            'secret': os.environ.get("SECRET_KEY")
        })

        self.crypto_purchased = defaultdict(float)
        self.total_investment = 0
        if os.environ.get("MODE") == '1':
            print('Running in mode 1...')
            schedule.every().day.at("00:00").do(self.place_all_orders)
            schedule.every().day.at("12:00").do(self.place_all_orders)

    def fetch_data(self, symbol, timeframe='1d', limit=20):
        try:
            since = self.exchange.milliseconds() - 86400000 * limit # default 20 days ago
            candles = self.exchange.fetch_ohlcv(symbol + '/' + self.currency, timeframe, since, limit)
            return candles
        except Exception as e:
            logging.error(f"An error occurred while fetching data: {e}")
            return None
      
    
    def calculate_bollinger_bands(self, close_prices, window_size=20, num_of_std=2):
        if len(close_prices) < window_size:
            logging.error(f"Insufficient data: {len(close_prices)}")
            logging.info(close_prices)
            raise ValueError("Insufficient data")
        close_prices_df = pd.DataFrame(close_prices)
        rolling_mean = close_prices_df.rolling(window=window_size).mean()
        rolling_std = close_prices_df.rolling(window=window_size).std()
        upper_band = rolling_mean + (rolling_std * num_of_std)
        lower_band = rolling_mean - (rolling_std * num_of_std)
        return rolling_mean.iloc[-1][0], upper_band.iloc[-1][0], lower_band.iloc[-1][0]


    def test(self):
        # markets = self.exchange.load_markets()
        # btcjpy = self.exchange.markets['BTC/JPY']
        # response = self.exchange.create_market_buy_order_with_cost('BTC/JPY', 100.8473)
        # # exchange.set_sandbox_mode(True)
        # #exchange.create_limit_buy_order('BTC/USDT', 0.001, 30000)
        # response = self.fetch_data('BTC/JPY')
        data = self.fetch_data('ETH')
        close_prices = [x[-2] for x in data]
        print(len(close_prices))
        print(self.calculate_bollinger_bands(close_prices))
        # print(self.exchange.fetch_ticker('ETH/JPY')['last'])
        # print(sum(map(float, self.symbols_ratio.values())))
        # for symbol, ratio in self.symbols_ratio.items():
        #     print(symbol, ratio)


    def place_order(self, symbol, ratio):
        cost = self.total_cost * ratio
        data = self.fetch_data(symbol) # ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        close_prices = [x[-2] for x in data]
        try:
            mean, upper_band ,lower_band = self.calculate_bollinger_bands(close_prices)
        except ValueError as e:
            logging.error(f"An error occurred while calculating bollinger bands for {symbol}: {e}")
            return
        market_price = self.exchange.fetch_ticker(symbol + '/' + self.currency)['last']

        if market_price >= upper_band:
            logging.info(f"Market price({market_price}) is above the upper band({upper_band}) for {symbol}. Selling...")
            try:
                response = self.exchange.create_market_sell_order_with_cost(symbol + '/' + self.currency, self.total_cost * ratio)
                self.total_investment -= response['cost']
                self.crypto_purchased[symbol] -= response['amount']
                logging.info(f'Sold {response["amount"]} {symbol} for {response["cost"]} JPY')
            except Exception as e:
                logging.error(f"An error occurred while placing sell order for {symbol}: {e}")
        else:
            price_weight = min(2, 2 * (upper_band - market_price) / (upper_band - lower_band))
            final_cost = self.total_cost * ratio * price_weight
            if final_cost <= self.minium_cost:
                logging.info(f"Final cost({final_cost}) is less than the minimum cost({self.minium_cost}) for {symbol}. Skipping...")
                return
            try:
                response = self.exchange.create_market_buy_order_with_cost(symbol + '/' + self.currency, final_cost)
                self.total_investment += response['cost']
                self.crypto_purchased[symbol] += response['amount']
                logging.info(f'Bought {response["amount"]} {symbol} for {response["cost"]} JPY')
            except Exception as e:
                logging.error(f"An error occurred while placing buy order for {symbol}: {e}")
    
    def place_all_orders(self):
        for symbol, ratio in self.symbols_ratio.items():
            time.sleep(1)
            self.place_order(symbol, float(ratio) / self.total_ratio)
        logging.info(f"Total investment: {self.total_investment} JPY")
        logging.info(f"Crypto purchased: {self.crypto_purchased}")
        self.calculate_profit()
    
    def calculate_profit(self):
        total_balance = 0
        for symbol, amount in self.crypto_purchased.items():
            market_price = self.exchange.fetch_ticker(symbol + '/' + self.currency)['last']
            total_balance += amount * market_price
            time.sleep(1)
        logging.info(f"Total balance: {total_balance} JPY")

    
    def run(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('Starting DCA bot...')
    dca = DCA('config.ini')
    dca.run()
