import pandas as pd
import tushare as ts
from notestock.dataset.dataset import StockBasic, TradeDay
from tqdm import tqdm

ts.set_token('79b91762c7f42780ccd697e5d228f28b446fb13a938e5012a2c1d25e')
pro = ts.pro_api()

trade = TradeDay()
basic = StockBasic()
trade.create()
basic.create()


def insert_basic():
    df0 = pro.stock_basic(exchange='', list_status='L')
    basic.insert_list(list(df0.to_dict(orient='index').values()))


def insert_day():
    df0 = pd.read_sql('select * from {}'.format(basic.table_name), basic.conn)
    for ts_code in tqdm(df0['ts_code'].values):
        df = ts.pro_bar(api=pro, ts_code=ts_code, asset='E',
                        freq='d', start_date='20000901', end_date='20201011')
        df['trade_time'] = df['trade_date']

        trade.insert_list(list(df.to_dict(orient='index').values()))


insert_basic()
trade.vacuum()
