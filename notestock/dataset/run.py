from notestock.dataset import StockDownload

root = '/Users/new/workspace/MyDiary/tmp/stocks/'
root = '/root/workspace/temp/stocks/'


def run_year():
    for year in range(2021, 2010, -1):
        path = root + '/years/stock-{}.db'.format(year)
        down = StockDownload(db_path=path)
        down.save_year(year)


def run_onebyone():
    path = root + '/stocks/'
    down = StockDownload(db_path=path + 'base.db')
    down.save_ones(path)


def run_month():
    for year in range(2000, 2021):
        for month in range(1, 13):
            month = year * 100 + month
            path = root + '/months/stock-{}.db'.format(month)
            down = StockDownload(db_path=path)
            down.save_month(month)
