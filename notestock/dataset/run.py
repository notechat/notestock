from notestock.dataset import StockDownload


def run1():
    for year in range(2021, 2010, -1):
        #year = 2019
        path = '/root/workspace/temp/stock-{}.db'.format(year)
        down = StockDownload(db_path=path)
        down.save_year(year)


def run2():
    path = '/root/workspace/temp/stocks/'
    down = StockDownload(db_path=path+'base.db')
    down.save_ones(path)


run2()
