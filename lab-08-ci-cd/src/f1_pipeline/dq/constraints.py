CONSTRAINTS = {
    "fact_results": {
        "not_null": ["result_id", "race_id", "driver_id", "constructor_id", "circuit_id", "points"],
        "checks": {
            "chk_grid_positive": "grid IS NULL or grid >= 1",
            "chk_points_non_negative": "points IS NULL OR points >= 0",
            "chk_laps_non_negative": "laps IS NULL OR laps >= 0",
            "chk_position_positive": "position IS NULL OR position >= 1",
        }
    },
    "dim_race": {
        "not_null": ["race_id", "season_year", "season_round"],
        "checks": {
            "chk_season_year": "season_year >= 1950",
            "chk_round": "season_round >= 1"
        }
    },
    "dim_circuit": {
        "not_null": ["circuit_id"],
        "checks": {
            "chk_lat": "lat IS NULL OR lat BETWEEN -90 AND 90",
            "chk_lng": "lng IS NULL OR lng BETWEEN -180 AND 180",
        }
    },
    "dim_constructor": {"not_null": ["constructor_id", "constructor_name"], "checks": {}},
    "dim_driver": {
        "not_null": ["driver_id", "valid_from"],
        "checks": {"chk_is_current": "is_current IN (0, 1)"},
    },
    "agg_driver_season": {
        "not_null": ["driver_id", "season_year"],
        "checks": {
            "chk_wins_le_races": "wins <= races_count",
            "chk_podiums_ge_wins": "podiums >= wins",
            "chk_podiums_le_races": "podiums <= races_count",
            "chk_points_non_negative": "points_sum >= 0",
        }
    },
    "agg_constructor_season": {
        "not_null": ["constructor_id", "season_year"],
        "checks": {
            "chk_wins_le_races": "wins <= races_count",
            "chk_podiums_ge_wins": "podiums >= wins",
            "chk_points_non_negative": "points_sum >= 0",
        }
    },
    "agg_driver_circuit": {
        "not_null": ["driver_id", "circuit_id"],
        "checks": {
            "chk_wins_le_races": "wins <= races_count",
            "chk_podiums_ge_wins": "podiums >= wins"
        }
    }
}


def apply_constraints(spark, table: str, constraints: dict):
    if not spark.catalog.tableExists(table):
        print(f"skip (table not found yet): {table}")
        return

    for col in constraints["not_null"]:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL")

    for name, expr in constraints["checks"].items():
        spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr})")
