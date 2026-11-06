from dataclasses import dataclass


@dataclass(frozen=True)
class Identity:
    user_name: str
    display_name: str
    host: str


@dataclass(frozen=True)
class ClusterSpec:
    name: str
    num_workers: int = 0
    autotermination_minutes: int = 10
    spark_version: str = None
    node_type_id: str = None

