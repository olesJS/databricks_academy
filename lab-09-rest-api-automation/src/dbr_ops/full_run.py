import logging
from pathlib import Path

from databricks.sdk import WorkspaceClient

from dbr_ops.models import FullRunReport, ClusterSpec
from dbr_ops.client import get_identity
from dbr_ops.clusters import create_cluster, delete_cluster
from dbr_ops.workspace import upload_notebook, delete_notebook
from dbr_ops.jobs import run_notebook, run_job
from dbr_ops.pipelines import run_pipeline

log = logging.getLogger(__name__)


def run_full_run(
    w: WorkspaceClient,
    spec: ClusterSpec,
    notebook_file: Path,
    job_ref: str | None = None,
    pipeline_ref: str | None = None,
    parameters: dict | None = None,
    timeout_s: int = 1800
) -> FullRunReport:
    report = FullRunReport(identity=get_identity(w))
    remote_path = None
    try:
        remote_path = upload_notebook(w, notebook_file, report.identity.user_name)

        if job_ref:
            report.runs.append(run_job(w, job_ref, timeout_s))

        if pipeline_ref:
            report.updates.append(run_pipeline(w, pipeline_ref, timeout_s=timeout_s))

        report.cluster_id = create_cluster(w, spec)
        report.runs.append(run_notebook(w, remote_path, report.cluster_id, timeout_s, parameters))
    except Exception as e:
        log.error("Full run step failed: %s", e)
        report.error = str(e)
    finally:
        if remote_path:
            try:
                delete_notebook(w, remote_path)
            except Exception as e:
                log.warning("Could not delete temporary notebook %s: %s", remote_path, e)

        if report.cluster_id:
            try:
                delete_cluster(w, report.cluster_id)
                report.cluster_deleted = True
            except Exception as e:
                log.error("Could not delete cluster %s: %s", report.cluster_id, e)

    return report

