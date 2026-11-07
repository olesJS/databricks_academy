import logging
import time
import dataclasses

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, SubmitTask

from dbr_ops.models import RunOutcome

log = logging.getLogger(__name__)


def _to_outcome(run) -> RunOutcome:
    st = run.state
    return RunOutcome(
        run_id=run.run_id,
        life_cycle_state=st.life_cycle_state.value if st and st.life_cycle_state else "UNKNOWN",
        result_state=st.result_state.value if st and st.result_state else None,
        url=run.run_page_url or "",
        message=(st.state_message or "") if st else "",
    )


def get_run_status(w: WorkspaceClient, run_id: int) -> RunOutcome:
    return _to_outcome(w.jobs.get_run(run_id=run_id))


def wait_for_run(w: WorkspaceClient, run_id: int, timeout_s: int = 1800, poll_s: int = 10) -> RunOutcome:
    deadline = time.time() + timeout_s

    last_state = None
    while True:
        outcome = get_run_status(w, run_id)

        if outcome.life_cycle_state != last_state:
            log.info("Run %s: %s", run_id, outcome.life_cycle_state)
            last_state = outcome.life_cycle_state

        if outcome.finished:
            return outcome

        if time.time() > deadline:
            log.error("Run %s timed out after %ss, cancelling", run_id, timeout_s)
            w.jobs.cancel_run(run_id=run_id)
            raise TimeoutError(f"Run {run_id} did not finish within {timeout_s}s")

        time.sleep(poll_s)


def resolve_job_id(w: WorkspaceClient, ref: str) -> int:
    if ref.isdigit():
        return int(ref)

    matches = list(w.jobs.list(name=ref))
    if not matches:
        raise LookupError(f"Job not found: {ref!r}")
    if len(matches) > 1:
        ids = ", ".join(str(j.job_id) for j in matches)
        raise LookupError(f"Several jobs named {ref!r} (ids: {ids}); use the id")
    return matches[0].job_id


def run_job(w: WorkspaceClient, ref: str, timeout_s: int = 1800) -> RunOutcome:
    job_id = resolve_job_id(w, ref)
    log.info("Triggering job %s", job_id)
    run_id = w.jobs.run_now(job_id=job_id).response.run_id
    log.info("Run started: %s", run_id)
    return wait_for_run(w, run_id, timeout_s)


def get_notebook_output(w: WorkspaceClient, run_id: int) -> str | None:
    run = w.jobs.get_run(run_id=run_id)
    if not run.tasks:
        return None
    out = w.jobs.get_run_output(run_id=run.tasks[0].run_id)
    return out.notebook_output.result if out.notebook_output else None


def run_notebook(
    w: WorkspaceClient,
    path: str,
    cluster_id: str | None = None,
    timeout_s: int = 1800,
    parameters: dict | None = None
) -> RunOutcome:
    task = SubmitTask(
        task_key="notebook",
        notebook_task=NotebookTask(notebook_path=path, base_parameters=parameters),
        existing_cluster_id=cluster_id  # None = serverless
    )
    log.info("Submitting notebook %s (%s)", path, f"cluster {cluster_id}" if cluster_id else "serverless")
    run_id = w.jobs.submit(run_name=f"dbrops: {path}", tasks=[task]).response.run_id
    log.info("Run started: %s", run_id)
    outcome = wait_for_run(w, run_id, timeout_s)
    if outcome.ok:
        outcome = dataclasses.replace(outcome, output=get_notebook_output(w, run_id))
    return outcome
