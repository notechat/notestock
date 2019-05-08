

# 股票总数
SELECT COUNT(1) FROM stock.stock_basic;

# 每只股票的个数
SELECT COUNT(1) FROM stock.stock_daily;

SELECT ts_code
      ,MIN(trade_date) AS "min"
      ,MAX(trade_date) AS "max"
FROM stock.stock_daily
GROUP BY ts_code
;

#DROP TABLE stock.stock_daily;