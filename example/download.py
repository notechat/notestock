from notestock.dataset import StockDownload

path = '/root/workspace/temp/stock-2020.db'
down = StockDownload(db_path=path)
down.save_year(2020)
