#standardSQL

# CTE to get the iqr and the percentiles
WITH iqr_t AS (
  SELECT
    percentile_25,
    percentile_75,
    (percentile_75 - percentile_25) AS iqr # The interquartile range
  FROM (# Calculate the 25th and 75th percentile
    SELECT
      APPROX_QUANTILES(total_sales, 4)[OFFSET (1)] AS percentile_25,
      APPROX_QUANTILES(total_sales, 4)[OFFSET (3)] AS percentile_75
    FROM ( # Get the total sales ($) by day to calculate the 25th and 75th percentile
      SELECT
        date,
        ROUND(SUM(sale_dollars), 2) AS total_sales
      FROM 
        `bigquery-public-data.iowa_liquor_sales.sales` 
      GROUP BY
        date)
      )
    )
 
# Primary query
# Get the date and the total sales by day from the main table, then get the percentiles from the CTE
# Then create a case statement to determine if the total sales for the day are 
# an outlier (either positive or negative) or an inlier
SELECT 
  date,
  total_sales,
  CASE # Check if the total sales for a day are above the 75th percentile + 1.5 x the iqr or if it is below the 25th percentile - 1.5 x the iqr
      WHEN total_sales >= percentile_75 + iqr * 1.5 THEN 'positive_outlier' 
      WHEN total_sales <= percentile_25 - iqr * 1.5 THEN 'negative_outlier' 
      ELSE 'inlier' 
  END AS type
FROM (# Get the total sales ($) by day from the main table
  SELECT
    date,
    ROUND(SUM(sale_dollars), 2) AS total_sales
  FROM 
    `bigquery-public-data.iowa_liquor_sales.sales`
  GROUP BY
    date),
  iqr_t
GROUP BY 
  date,
  total_sales,
  percentile_75,
  percentile_25,
  iqr
ORDER BY
  total_sales DESC