CREATE OR REPLACE VIEW gold_fx_daily AS
SELECT date, base_ccy, quote_ccy, rate FROM silver_fx;

CREATE OR REPLACE VIEW gold_crypto_daily AS
SELECT date_trunc('day', ts) AS date, symbol, quote_ccy, avg(price) AS avg_price
FROM silver_crypto
GROUP BY 1,2,3;

CREATE OR REPLACE VIEW gold_crypto_fx_aligned AS
SELECT c.date, c.symbol, c.quote_ccy, c.avg_price, f.rate
FROM gold_crypto_daily c
JOIN gold_fx_daily f
  ON f.date = c.date AND f.quote_ccy = c.quote_ccy;
