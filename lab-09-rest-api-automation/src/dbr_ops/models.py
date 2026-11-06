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
    spark_version: str | None = None
    node_type_id: str | None = None


@dataclass(frozen=True)
class RunOutcome:
    run_id: int
    life_cycle_state: str
    result_state: str | None
    url: str
    message: str = ""

    @property
    def finished(self) -> bool:
        return self.life_cycle_state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

    @property
    def ok(self) -> bool:
        return self.result_state == "SUCCESS"
