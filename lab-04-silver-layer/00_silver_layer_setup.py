# Databricks notebook source
CATALOG_NAME = "dbr_dev_ua5816bd"
BRONZE_SCHEMA = f"{CATALOG_NAME}.oles0305_bronze"
SILVER_SCHEMA = f"{CATALOG_NAME}.oles0305_silver"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.drivers (
        driverId INT, 
        driverRef STRING, 
        number INT, 
        code STRING, 
        forename STRING, 
        surname STRING, 
        dob DATE, 
        nationality STRING, 
        url STRING,
        bronze_ingested_at TIMESTAMP,
        valid_from TIMESTAMP, 
        valid_to TIMESTAMP, 
        is_active BOOLEAN
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.results (
        resultId INT, 
        raceId INT, 
        driverId INT, 
        constructorId INT,
        grid INT, 
        position INT, 
        points DOUBLE, 
        laps INT, 
        milliseconds INT,
        statusId INT, 
        time STRING,
        bronze_ingested_at TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (raceId, driverId) -- Liquid Clustering
""")

# COMMAND ----------

spark.sql(f"ALTER TABLE {SILVER_SCHEMA}.results ADD CONSTRAINT IF NOT EXISTS valid_points CHECK (points >= 0)")
spark.sql(f"ALTER TABLE {SILVER_SCHEMA}.results ADD CONSTRAINT IF NOT EXISTS valid_ids CHECK (driverId IS NOT NULL AND raceId IS NOT NULL)")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.sprint_results (
        resultId INT, 
        raceId INT, 
        driverId INT, 
        constructorId INT,
        grid INT, 
        position INT, 
        points DOUBLE, 
        laps INT, 
        time STRING,
        milliseconds INT,
        statusId INT, 
        evh_ingested_at TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (raceId, resultId) -- Liquid Clustering
""")

# COMMAND ----------

spark.sql(f"ALTER TABLE {SILVER_SCHEMA}.sprint_results ADD CONSTRAINT IF NOT EXISTS valid_points CHECK (points >= 0)")
spark.sql(f"ALTER TABLE {SILVER_SCHEMA}.sprint_results ADD CONSTRAINT IF NOT EXISTS valid_ids CHECK (driverId IS NOT NULL AND raceId IS NOT NULL)")