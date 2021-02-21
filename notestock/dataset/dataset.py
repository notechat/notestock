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


class QuotationDay(SqliteTable):
    def __init__(self, table_name='quotation_day', db_path=None, *args, **kwargs):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/stock.db'

        super(QuotationDay, self).__init__(
            db_path=db_path, table_name=table_name, *args, **kwargs)
        self.columns = ['ts_code', 'date', 'time', 'open', 'high',
                        'low', 'close', 'volume', 'amount', 'pre_close']

    def create(self):
        self.execute("""
            create table if not exists {} (
               ts_code       VARCHAR(255)
              ,date          VARCHAR(255)
              ,time          VARCHAR(255)
              ,open          FLOAT
              ,high          FLOAT
              ,low           FLOAT
              ,close         FLOAT
              ,volume        FLOAT
              ,amount        FLOAT
              ,pre_close     FLOAT   
              ,primary key (ts_code,time)           
              )
            """.format(self.table_name))


class QuotationMin(SqliteTable):
    def __init__(self, table_name='quotation_min', db_path=None, *args, **kwargs):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/stock.db'

        super(QuotationMin, self).__init__(
            db_path=db_path, table_name=table_name, *args, **kwargs)
        self.columns = ['ts_code', 'date', 'time', 'open',
                        'high', 'low', 'close', 'volume', 'amount']
        self.create()

    def create(self):
        self.execute("""
            create table if not exists {} (
               ts_code       VARCHAR(255)
              ,date          VARCHAR(255)
              ,time          VARCHAR(255)
              ,open          FLOAT
              ,high          FLOAT
              ,low           FLOAT
              ,close         FLOAT
              ,volume        FLOAT
              ,amount        FLOAT                 
              ,primary key (ts_code,time)           
              )
            """.format(self.table_name))


class QuotationMin1(QuotationMin):
    def __init__(self, table_name='quotation_min1',  *args, **kwargs):
        super(QuotationMin1, self).__init__(
            table_name=table_name, *args, **kwargs)


class QuotationMin5(QuotationMin):
    def __init__(self, table_name='quotation_min5',  *args, **kwargs):
        super(QuotationMin5, self).__init__(
            table_name=table_name, *args, **kwargs)


class QuotationMin15(QuotationMin):
    def __init__(self, table_name='quotation_min15',  *args, **kwargs):
        super(QuotationMin15, self).__init__(
            table_name=table_name, *args, **kwargs)


class QuotationMin30(QuotationMin):
    def __init__(self, table_name='quotation_min30',  *args, **kwargs):
        super(QuotationMin30, self).__init__(
            table_name=table_name, *args, **kwargs)


class QuotationMin60(QuotationMin):
    def __init__(self, table_name='quotation_min60',  *args, **kwargs):
        super(QuotationMin60, self).__init__(
            table_name=table_name, *args, **kwargs)


class TradeDetail(SqliteTable):
    def __init__(self, table_name='trade_detail', db_path=None, *args, **kwargs):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/stock.db'

        super(TradeDetail, self).__init__(db_path=db_path,
                                          table_name=table_name, *args, **kwargs)
        self.columns = ['ts_code', 'trade_date', 'trade_time',
                        'price', 'price_mod', 'vol', 'amount']

    def create(self):
        self.execute("""
            create table if not exists {} (
               ts_code       VARCHAR(255)
              ,trade_date    VARCHAR(255)
              ,trade_time    VARCHAR(255)
              
              ,price         FLOAT
              ,price_mod     FLOAT
              ,vol           FLOAT
              ,amount        FLOAT
              ,primary key (ts_code,trade_time)           
              )
            """.format(self.table_name))
