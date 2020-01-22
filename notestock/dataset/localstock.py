#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/04/04 00:31
# @Author  : niuliangtao
# @Site    : 
# @File    : mysql.py
# @Software: PyCharm
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts
from dateutil.relativedelta import relativedelta
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
        self.path_code = '{}/daily'.format(self.path_base)

        self.path_month = '{}/month'.format(self.path_base)
        self.path_year = '{}/year'.format(self.path_base)

        self.file_info = '{}/stock_info.txt'.format(self.path_info)

    def stock_basic_create(self):
        try:
            if not os.path.exists(self.path_base):
                os.makedirs(self.path_base)
            if not os.path.exists(self.path_info):
                os.makedirs(self.path_info)
            if not os.path.exists(self.path_code):
                os.makedirs(self.path_code)
            if not os.path.exists(self.path_month):
                os.makedirs(self.path_month)

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

    def stock_daily_updated_merge(self, start_date='2015', end_date='2019'):
        start_time = datetime.strptime(start_date, '%Y')
        end_time = datetime.strptime(end_date, '%Y')

        dfs = []
        last_year = start_time.year
        while start_time < end_time:

            month_path = '{}/{}.csv'.format(self.path_month, start_time.strftime('%Y%m'))

            if start_time.year != last_year:
                last_year = start_time.year
                df = pd.concat(dfs, axis=0, ignore_index=True)
                dfs = []
                self.write(df, '{}/{}.csv'.format(self.path_year, start_time.strftime('%Y')), overwrite=True)

            dfs.append(pd.read_csv(month_path, index_col=0))

            print(start_time)
            start_time += relativedelta(months=1)
        if len(dfs) > 0:
            df = pd.concat(dfs, axis=0, ignore_index=True)

            self.write(df, '{}/{}.csv'.format(self.path_year, start_time.strftime('%Y')), overwrite=True)

        # start_time.strftime('%Y%m%d')
        print(start_time)

        pass

    def stock_daily_updated_days(self, start_date='20190401', end_date='20190430'):
        start_time = datetime.strptime(start_date, '%Y%m%d')
        end_time = datetime.strptime(end_date, '%Y%m%d')

        s = end_time - start_time

        with tqdm(total=s.days, desc="download daily", unit='次') as pbar:
            while start_time < end_time:
                self.stock_daily_updated_day(trade_date=start_time.strftime('%Y%m%d'))
                start_time += timedelta(1)
                pbar.update(1)
        pass

    def stock_daily_updated_day(self, trade_date='20191111'):
        trade_time = datetime.strptime(trade_date, '%Y%m%d')
        file_path = '{}/{}.csv'.format(self.path_month, trade_time.strftime('%Y%m'))

        times = 5
        while times > 0:
            try:
                data = self.pro.daily(trade_date=trade_date)
                self.write(data, file_path)
                return
            except Exception as e:
                times -= 1
                print('try again {}'.format(e))
                time.sleep(10)

    def stock_daily_updated_one(self, ts_code='000001.SH', start_date='20150101', end_date='20190405', freq='D'):
        file_path = '{}/{}.csv'.format(self.path_code, ts_code)
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

    @staticmethod
    def write(pd_df, path, overwrite=False):
        file_dir, file_path = os.path.split(path)
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)

        if os.path.exists(path) and not overwrite:
            pd_df.to_csv(path, mode='a+', header=False)
        else:
            pd_df.to_csv(path)
