from huobi.client.account import AccountClient
from huobi.client.generic import CandlestickInterval, GenericClient
from huobi.client.market import LogInfo, MarketClient
from huobi.client.trade import TradeClient

generic_client = GenericClient()
list_symbol = generic_client.get_exchange_symbols()
list_currency = generic_client.get_reference_currencies()
print(list_symbol[0])
print(list_currency[0].print_object())


a = c
access_key = " "
secret_key = " "

# Create generic client instance and get the timestamp
generic_client = GenericClient()
timestamp = generic_client.get_exchange_timestamp()
print(timestamp)

# Create the market client instance and get the latest btcusdt‘s candlestick
market_client = MarketClient()
list_obj = market_client.get_candlestick(
    "btcusdt", CandlestickInterval.MIN5, 10)
LogInfo.output_list(list_obj)

# // Create an AccountClient instance with APIKey
account_client = AccountClient(api_key=access_key, secret_key=secret_key)

# // Create a TradeClient instance with API Key and customized host
trade_client = TradeClient(
    api_key=access_key, secret_key=secret_key, url="https://api-aws.huobi.pro")
