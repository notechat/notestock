#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/04/04 00:31
# @Author  : niuliangtao
# @Site    : 
# @File    : mysql.py
# @Software: PyCharm

import os
import time

import tushare as ts
from notetool.tool import SecretManage
from tqdm import tqdm


def read_local(token):
    secret = SecretManage(key='ts_token', value=token, path='stock')
    ts.set_token(secret.read())
    pass


class DatabaseStock:
    def __init__(self, connection=None, token=None, path_base='tmp'):
        read_local(token=token)
        self.pro = ts.pro_api()
        self.connect = connection

        self.path_base = path_base

        self.path_info = '{}/info'.format(self.path_base)
        self.path_daily = '{}/daily'.format(self.path_base)

        self.file_info = '{}/stock_info.txt'.format(self.path_info)

    def stock_basic_create(self):
        try:
            if not os.path.exists(self.path_base):
                os.makedirs(self.path_base)
            if not os.path.exists(self.path_info):
                os.makedirs(self.path_info)
            if not os.path.exists(self.path_daily):
                os.makedirs(self.path_daily)
        except Exception as e:
            print(e)

    def stock_basic_updated_data(self):
        try:

            fields = "ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type," \
                     "list_status,list_date,delist_date,is_hs"
            data = self.pro.stock_basic(exchange='', list_status='L', fields=fields)
            data.to_csv(self.file_info)
        except Exception as e:
            print(e)

    def stock_daily_updated_one(self, ts_code='000001.SH', start_date='20150101', end_date='20190405', freq='D'):

        file_path = '{}/{}.csv'.format(self.path_daily, ts_code)
        if os.path.exists(file_path):
            return

        times = 5
        while times > 0:
            try:
                end_date = end_date or time.strftime("%Y%m%d", time.localtime())
                data = ts.pro_bar(api=self.pro, ts_code=ts_code, asset='E', freq=freq, start_date=start_date,
                                  end_date=end_date)
                data.to_csv(file_path)
                # print('{} daily download success.'.format(ts_code))
                return
            except Exception as e:
                times -= 1
                print('try again {}'.format(e))
                time.sleep(10)

    def stock_daily_updated_all(self, start_date='20150101', end_date='20190405', freq='5min'):
        data = self.pro.stock_basic(exchange='', list_status='L', fields="ts_code")

        for line in tqdm(iterable=data.values, total=data.shape[0], unit='个', desc=''):
            self.stock_daily_updated_one(ts_code=line[0], start_date=start_date, end_date=end_date, freq=freq)

    def stock_min_updated_one(self, ts_code='000001.SH', start_date='20150101', end_date='20190405', freq='5min'):
        total_line = 0
        try:
            end_date = end_date or time.strftime("%Y%m%d", time.localtime())
            with self.connect.cursor() as cursor:
                fields = "ts_code,trade_time,open,high,low,close,vol,amount,trade_date,pre_close"
                param2 = '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s'
                sql = 'REPLACE INTO stock_daily ({}) VALUES ({})'.format(fields, param2)

                data = ts.pro_bar(api=self.pro, ts_code=ts_code, asset='E', freq=freq, start_date=start_date,
                                  end_date=end_date)

                if data is None:
                    return 0

                if 'min' not in freq:
                    data = data[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'trade_date',
                                 'pre_close']]

                data = data.fillna(0)

                for line in data.values:
                    cursor.execute(sql, tuple(line))
                total_line = len(data.values)

                print("{} from {} to {} {} Done".format(ts_code, start_date, end_date, total_line))
            self.connect.commit()
        except Exception as e:
            print(e)
        return total_line

    def stock_min_updated_all(self, start_date='20150101', end_date='20190405', freq='5min'):
        data = self.pro.stock_basic(exchange='', list_status='L', fields="ts_code")
        for line in data.values:
            ts_code = line[0]
            self.stock_min_updated_one(ts_code=ts_code, start_date=start_date, end_date=end_date, freq=freq)
