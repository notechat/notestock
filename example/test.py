from notestock.dataset.localstock import DatabaseStock

from notedrive.baidu.drive import BaiDuDrive

df = DatabaseStock(path_base='/Users/liangtaoniu/workspace/MyDiary/tmp/stock')

df.stock_basic_create()
df.stock_basic_updated_data()
# df.stock_daily_updated_all(freq='D', end_date="")
# df.stock_daily_updated_days(start_date='20150101', end_date='20191119')
# df.stock_daily_updated_merge()

# client = BaiDuDrive()
# client.upload_dir('/Users/liangtaoniu/workspace/MyDiary/tmp/stock/month', '/drive/stock/month')

df = DatabaseStock(path_base='/Users/liangtaoniu/workspace/MyDiary/tmp/stock')

df.stock_basic_create()
df.stock_basic_updated_data()
# df.stock_daily_updated_all(freq='D', end_date="")
# df.stock_daily_updated_days(start_date='20150101', end_date='20191119')
# df.stock_daily_updated_merge()

client = BaiDuDrive()
client.upload_dir('/Users/liangtaoniu/workspace/MyDiary/tmp/stock/year', '/drive/stock/year')
