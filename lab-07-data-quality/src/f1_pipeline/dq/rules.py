from pyspark.sql import functions as F
from pyspark.sql import DataFrame


RESULTS_RULES = {
    "valid_result_id": "resultId IS NOT NULL",
    "valid_race_id": "raceId IS NOT NULL",
    "valid_driver_id": "driverId IS NOT NULL",
    "valid_constructor_id": "constructorId IS NOT NULL",
    "valid_grid": "grid IS NOT NULL AND grid >= 0",
    "valid_position_order": "positionOrder IS NOT NULL",
    "valid_position": "position IS NULL OR position >= 1", 
    "valid_points": "points IS NOT NULL AND points >= 0",
    "valid_laps": "laps IS NULL OR laps >= 0",
    "valid_speed": "fastestLapSpeed IS NULL OR fastestLapSpeed > 0",
}

RACES_RULES = {
    "valid_race_id": "raceId IS NOT NULL",
    "valid_circuit_id": "circuitId IS NOT NULL",
    "valid_year": "year BETWEEN 1950 AND year(current_date())",
    "valid_round": "round >= 1",
    "valid_date": "date IS NOT NULL",
}

CIRCUITS_RULES = {
    "valid_circuit_id": "circuitId IS NOT NULL"
}

CIRCUITS_WARN = {
    "valid_coordinates": "lat BETWEEN -90 AND 90 AND lng BETWEEN -180 AND 180"
}

CONSTRUCTORS_RULES = {
    "valid_constructor_id": "constructorId IS NOT NULL",
    "valid_name": "name IS NOT NULL",
}


def violation_condition(rules: dict) -> str:
    return " OR ".join(f"NOT COALESCE({rule}, FALSE)" for rule in rules.values())


def add_failed_rules(df: DataFrame, rules: dict) -> DataFrame:
    df = df.where(F.expr(violation_condition(rules)))
    failed = F.array(*[
        F.when(F.coalesce(F.expr(rule), F.lit(False)), name) for name, rule in rules.items()
    ])
    return df.withColumn("_failed_rules", F.filter(failed, lambda x: x.isNotNull()))
