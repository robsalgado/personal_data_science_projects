-- CTE to join our portfolio with the
-- daily closing prices

WITH prices_joined AS (
  SELECT
    strat_log.symbol AS symbol,
    qty,
    strat_log.date AS date,
    closePrice * qty AS market_value
  FROM
    `<YOUR PROJECT>.equity_data.strategy_log` AS strat_log
  JOIN (
    SELECT
      closePrice,
      symbol,
      date
      FROM
      `<YOUR PROJECT>.equity_data.daily_quote`
      ) AS price_data
  ON 
    strat_log.symbol = price_data.symbol
    AND strat_log.date = price_data.date
  WHERE
    strat = 'momentum_strat_1'
)

-- Main query to get the portfolio total
-- by day along with the percent change 
-- from the previous day
SELECT 
  date,
  ROUND(total, 2) AS total,
  ROUND((total / prev_day -1) * 100, 2) AS perc_diff
FROM (
  SELECT
    date,
    SUM(market_value) AS total,
    LAG(SUM(market_value)) OVER (ORDER BY date) AS prev_day
  FROM
    prices_joined
  GROUP BY
    date
    )
ORDER BY 
  date