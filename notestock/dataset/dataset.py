import os

from notedata.tables import SqliteTable


class StockBasic(SqliteTable):
    def __init__(self, table_name='stock_basic', db_path=None, *args, **kwargs):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/stock.db'

        super(StockBasic, self).__init__(db_path=db_path,
                                         table_name=table_name, *args, **kwargs)
        self.columns = ['ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname', 'market', 'exchange',
                        'curr_type', 'list_status', 'list_date', 'delist_date', 'is_hs'
                        ]

    def create(self):
        self.execute("""
                create table if not exists {} (
                 ts_code       VARCHAR(255)
                ,symbol        VARCHAR(255)
                ,name          VARCHAR(255)
                ,area          VARCHAR(255)
                ,industry      VARCHAR(255)
                ,fullname      VARCHAR(255)
                ,enname        VARCHAR(255)
                ,market        VARCHAR(255)
                ,exchange      VARCHAR(255)
                ,curr_type     VARCHAR(255)
                ,list_status   VARCHAR(255)
                ,list_date     VARCHAR(255)
                ,delist_date   VARCHAR(255)
                ,is_hs         VARCHAR(255)
                ,PRIMARY KEY (ts_code)
                )
                """.format(self.table_name))


class TradeDay(SqliteTable):
    def __init__(self, table_name='trade_day', db_path=None, *args, **kwargs):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/stock.db'

        super(TradeDay, self).__init__(db_path=db_path,
                                       table_name=table_name, *args, **kwargs)
        self.columns = ['ts_code', 'trade_time', 'open', 'high', 'low', 'close', 'vol', 'amount', 'trade_date',
                        'pre_close']

    def create(self):
        self.execute("""
            create table if not exists {} (
               ts_code       VARCHAR(255)
              ,trade_time    VARCHAR(255)
              ,open          FLOAT
              ,high          FLOAT
              ,low           FLOAT
              ,close         FLOAT
              ,vol           FLOAT
              ,amount        FLOAT
              ,trade_date    VARCHAR(255)
              ,pre_close     FLOAT   
              ,primary key (ts_code,trade_time)           
              )
            """.format(self.table_name))


class TradeMin(TradeDay):
    def __init__(self, table_name='trade_min', *args, **kwargs):
        super(TradeMin, self).__init__(table_name=table_name, *args, **kwargs)
