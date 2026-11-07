import logging
import time

from databricks.sdk import WorkspaceClient

from dbr_ops.models import UpdateOutcome

log = logging.getLogger(__name__)


def resolve_pipeline_id(w: WorkspaceClient, ref: str) -> str:
    ids = [p.pipeline_id for p in w.pipelines.list_pipelines() if ref in (p.name, p.pipeline_id)]
    if not ids:
        raise LookupError(f"Pipeline not found: {ref!r}")
    if len(ids) > 1:
        raise LookupError(f"Several pipelines match {ref!r} (ids: {', '.join(ids)}); use the id")
    return ids[0]


def _get_outcome(w: WorkspaceClient, pipeline_id: str, update_id: str) -> UpdateOutcome:
    info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update
    return UpdateOutcome(
        pipeline_id=pipeline_id,
        update_id=update_id,
        state=info.state.value if info and info.state else "UNKNOWN",
        url=f"{w.config.host}/pipelines/{pipeline_id}/updates/{update_id}",
    )


def run_pipeline(
    w: WorkspaceClient,
    ref: str,
    full_refresh: bool = False,
    timeout_s: int = 1800
) -> UpdateOutcome:
    poll_s: int = 10
    pipeline_id = resolve_pipeline_id(w, ref)
    log.info("Starting update of pipeline %s (full_refresh=%s)", pipeline_id, full_refresh)
    update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh).update_id
    log.info("Update started: %s", update_id)

    deadline = time.time() + timeout_s
    last_state = None
    while True:
        outcome = _get_outcome(w, pipeline_id, update_id)

        if outcome.state != last_state:
            log.info("Update %s: %s", update_id, outcome.state)
            last_state = outcome.state

        if outcome.finished:
            return outcome

        if time.time() > deadline:
            log.error("Update %s timed out after %ss, stopping pipeline", update_id, timeout_s)
            w.pipelines.stop(pipeline_id=pipeline_id)
            raise TimeoutError(f"Pipeline update {update_id} did not finish within {timeout_s}s")

        time.sleep(poll_s)
