from notestock.dataset import StockDownload

for year in range(2010, 2020):
    #year = 2019
    path = '/root/workspace/temp/stock-{}.db'.format(year)
    down = StockDownload(db_path=path)
    down.save_year(year)
