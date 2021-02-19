import os

import pandas as pd
import tushare as ts
from notestock.dataset.dataset import StockBasic, TradeDay
from notetool import log
from tqdm import tqdm

logger = log("stock")


class StockDownload:
    def __init__(self, db_path=None):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/stock.db'

        ts.set_token(
            '79b91762c7f42780ccd697e5d228f28b446fb13a938e5012a2c1d25e')
        self.pro = ts.pro_api()
        self.trade = TradeDay(db_path=db_path)
        self.basic = StockBasic(db_path=db_path)
        self.trade.create()
        self.basic.create()

    def insert_basic(self):
        stock_info = self.pro.stock_basic(exchange='', list_status='L')
        response = self.basic.insert_list(
            list(stock_info.to_dict(orient='index').values()))
        logger.info("update stock info {} rows {}".format(
            len(stock_info), response))

    def insert_day(self, start_date='20000901', end_date='20211011'):
        info = pd.read_sql(
            'select * from {}'.format(self.basic.table_name), self.basic.conn)

        for ts_code in tqdm(info['ts_code'].values):
            while (True):
                try:
                    df = ts.pro_bar(api=self.pro,
                                    ts_code=ts_code,
                                    asset='E',
                                    freq='d',
                                    start_date=start_date,
                                    end_date=end_date)
                    if df is None:
                        continue
                    df['trade_time'] = df['trade_date']
                    self.trade.insert_list(
                        list(df.to_dict(orient='index').values()))
                    break
                except Exception as e:
                    pass
        self.trade.vacuum()

    def save_year(self, year=2020):
        self.insert_basic()

        start_date = '{}0101'.format(year)
        end_date = '{}1231'.format(year)
        self.insert_day(start_date, end_date)

        self.trade.vacuum()
