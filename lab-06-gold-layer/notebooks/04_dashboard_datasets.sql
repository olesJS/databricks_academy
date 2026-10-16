-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### driver_standing

-- COMMAND ----------

SELECT ads.driver_id
    , ads.season_year
    , d.driver_code
    , CONCAT(d.forename, " ", d.surname) AS driver_name
    , ads.points_sum AS points
    , ads.wins
    , ads.podiums
    , ads.races_count
    , RANK() OVER (PARTITION BY ads.season_year ORDER BY ads.points_sum DESC, ads.wins DESC) as rnk
FROM agg_driver_season ads
JOIN dim_driver d ON ads.driver_id = d.driver_id AND d.is_current = 1
ORDER BY ads.season_year DESC, ads.points_sum DESC, ads.wins DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### constructor_standing

-- COMMAND ----------

SELECT a.constructor_id
    , a.season_year
    , a.races_count
    , a.wins
    , a.podiums
    , a.points_sum AS points
    , c.constructor_ref
    , c.constructor_name
    , a.points_sum / NULLIF(SUM(a.points_sum) OVER (PARTITION BY a.season_year), 0) AS points_share
FROM agg_constructor_season a
JOIN dim_constructor c ON a.constructor_id = c.constructor_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### circuit_stats

-- COMMAND ----------

SELECT a.races_count
    , a.wins
    , a.podiums
    , a.points_sum AS points
    , a.grid_position_sum
    , a.position_order_sum
    , d.driver_id
    , d.driver_code
    , CONCAT_WS(' ', d.forename, d.surname) AS driver_name
    , c.*
FROM agg_driver_circuit a
JOIN dim_circuit c ON a.circuit_id = c.circuit_id
JOIN dim_driver d ON a.driver_id = d.driver_id AND d.is_current = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### race_results

-- COMMAND ----------

SELECT
    f.result_id, f.race_id, f.driver_id, f.constructor_id, f.circuit_id,
    r.season_year, r.season_round, r.race_name, r.race_date,
    d.driver_code,
    CONCAT_WS(' ', d.forename, d.surname) AS driver_name,
    co.constructor_name,
    ci.*,
    f.grid,
    f.position_order,
    f.points,
    CASE 
        WHEN f.position_order = 1  THEN 1 ELSE 0 
    END AS is_win,
    CASE 
        WHEN f.position_order <= 3 THEN 1 ELSE 0 END 
    AS is_podium,
    f.grid - f.position_order AS positions_gained,
    SUM(f.points) OVER (
        PARTITION BY r.season_year, f.driver_id
        ORDER BY r.season_round
    ) AS cumulative_points
FROM fact_results f
JOIN dim_race r ON f.race_id = r.race_id
JOIN dim_constructor co ON f.constructor_id = co.constructor_id
JOIN dim_circuit ci ON f.circuit_id = ci.circuit_id
JOIN dim_driver d ON f.driver_id = d.driver_id AND d.is_current = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### start_vs_finish

-- COMMAND ----------

SELECT
  COALESCE(d.driver_code, d.driver_ref) AS driver_code,
  CONCAT_WS(' ', d.forename, d.surname) AS driver_name,
  ci.country,
  ci.circuit_name,
  COUNT(*)                  AS races_count,
  AVG(f.grid)               AS avg_grid,
  AVG(f.position_order)     AS avg_finish
FROM dbr_dev_ua5816bd.oles0305_gold.fact_results f
JOIN dbr_dev_ua5816bd.oles0305_gold.dim_driver d
  ON f.driver_id = d.driver_id AND d.is_current = 1
JOIN dbr_dev_ua5816bd.oles0305_gold.dim_circuit ci
  ON f.circuit_id = ci.circuit_id
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) >= 5