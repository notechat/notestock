import os
import time

import pandas as pd
import tushare as ts
from notestock.dataset.dataset import (QuotationDay, QuotationMin1,
                                       QuotationMin5, QuotationMin15,
                                       QuotationMin30, QuotationMin60,
                                       StockBasic)
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
        self.quotation_day = QuotationDay(db_path=db_path)
        self.quotation_min1 = QuotationMin1(db_path=db_path)
        self.quotation_min5 = QuotationMin5(db_path=db_path)
        self.quotation_min15 = QuotationMin15(db_path=db_path)
        self.quotation_min30 = QuotationMin30(db_path=db_path)
        self.quotation_min60 = QuotationMin60(db_path=db_path)
        self.basic = StockBasic(db_path=db_path)

        self.quotation_day.create()
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
            while True:
                try:
                    df = ts.pro_bar(api=self.pro,
                                    ts_code=ts_code,
                                    asset='E',
                                    freq='d',
                                    start_date=start_date,
                                    end_date=end_date)
                    if df is None:
                        time.sleep(10)
                        continue
                    df['trade_time'] = df['trade_date']
                    df = df.rename(columns={
                        'trade_date': 'date',
                        'trade_time': 'time',
                        'vol': 'volume',
                    })
                    self.quotation_day.insert_list(
                        list(df.to_dict(orient='index').values()))
                    break
                except Exception as e:
                    time.sleep(10)
        self.quotation_day.vacuum()

    def insert_min5(self, start_date='20000901', end_date='20211011'):
        import baostock as bs
        import pandas as pd

        # 登陆系统 ####
        lg = bs.login()
        # 显示登陆返回信息
        print('login respond error_code:' + lg.error_code)
        print('login respond  error_msg:' + lg.error_msg)

        info = pd.read_sql(
            'select * from {}'.format(self.basic.table_name), self.basic.conn)

        start_date = '{}-{}-{}'.format(start_date[:4],
                                       start_date[4:6], start_date[6:])
        end_date = '{}-{}-{}'.format(end_date[:4], end_date[4:6], end_date[6:])
        # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
        # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
        fields = "date,time,code,open,high,low,close,volume,amount"
        for ts_code in tqdm(info['ts_code'].values):
            while True:
                try:
                    code = ts_code.lower().split('.')
                    code = '{}.{}'.format(code[1], code[0])
                    rs = bs.query_history_k_data_plus(code,
                                                      fields=fields,
                                                      start_date=start_date,
                                                      end_date=end_date,
                                                      frequency="5",
                                                      adjustflag="3")
                    df = pd.DataFrame(rs.get_data(), columns=rs.fields)

                    if rs.error_code != '0':
                        logger.error('query_history_k_data_plus respond error_code:{},error_msg:{}'.format(
                            rs.error_code, rs.error_msg))
                        continue

                    if df is None:
                        time.sleep(10)
                        continue

                    df['ts_code'] = ts_code
                    self.quotation_min5.insert_list(
                        list(df.to_dict(orient='index').values()))
                    break
                except Exception as e:
                    time.sleep(10)
        self.quotation_min5.vacuum()
        # 登出系统 #
        bs.logout()

    def save_year(self, year=2020):
        self.insert_basic()

        start_date = '{}0101'.format(year)
        end_date = '{}1231'.format(year)
        self.insert_min5(start_date=start_date, end_date=end_date)
        self.insert_day(start_date, end_date)

        self.quotation_day.vacuum()
