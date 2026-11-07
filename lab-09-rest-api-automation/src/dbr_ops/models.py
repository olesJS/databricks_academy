from dataclasses import dataclass, field


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
    policy_id: str | None = None


@dataclass(frozen=True)
class RunOutcome:
    run_id: int
    life_cycle_state: str
    result_state: str | None
    url: str
    message: str = ""
    output: str | None = None

    @property
    def finished(self) -> bool:
        return self.life_cycle_state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

    @property
    def ok(self) -> bool:
        return self.result_state == "SUCCESS"


@dataclass(frozen=True)
class UpdateOutcome:
    pipeline_id: str
    update_id: str
    state: str
    url: str

    @property
    def finished(self) -> bool:
        return self.state in {"COMPLETED", "FAILED", "CANCELED"}

    @property
    def ok(self) -> bool:
        return self.state == "COMPLETED"


@dataclass()
class FullRunReport:
    identity: Identity = None
    cluster_id: str = None
    runs: list[RunOutcome] = field(default_factory=list)
    updates: list[UpdateOutcome] = field(default_factory=list)
    cluster_deleted: bool = False
    error: str = None

    @property
    def ok(self) -> bool:
        return (
            self.error is None
            and self.cluster_deleted
            and bool(self.runs or self.updates)
            and all(r.ok for r in self.runs)
            and all(u.ok for u in self.updates)
        )
