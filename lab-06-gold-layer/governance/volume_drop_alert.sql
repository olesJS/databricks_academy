WITH ranked AS (
  SELECT rows_num
    , LAG(rows_num) OVER (ORDER BY log_ts) AS prev_rows
    , ROW_NUMBER()  OVER (ORDER BY log_ts DESC) AS rn
  FROM dbr_dev_ua5816bd.oles0305_gold.data_volume_log
  WHERE table_name = 'fact_results'
)

SELECT rows_num AS curr_rows
  , prev_rows
  , COALESCE(100.0 * (rows_num - prev_rows) / NULLIF(prev_rows, 0), 0) AS volume_change_pct
FROM ranked
WHERE rn = 1;