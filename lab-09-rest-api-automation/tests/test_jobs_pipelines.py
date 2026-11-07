from types import SimpleNamespace as ns
from unittest.mock import MagicMock

import pytest

from dbr_ops import jobs
from dbr_ops.pipelines import resolve_pipeline_id

PID = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def w():
    return MagicMock()


def make_run(lc, rs=None, msg=""):
    state = ns(
        life_cycle_state=ns(value=lc),
        result_state=ns(value=rs) if rs else None,
        state_message=msg,
    )
    return ns(run_id=1, state=state, run_page_url="http://run/1")


def test_wait_for_run_returns_when_finished(w):
    w.jobs.get_run.side_effect = [
        make_run("PENDING"), make_run("RUNNING"), make_run("TERMINATED", "SUCCESS"),
    ]
    outcome = jobs.wait_for_run(w, 1, timeout_s=60, poll_s=0)
    assert outcome.ok and outcome.finished


def test_wait_for_run_reports_failure(w):
    w.jobs.get_run.return_value = make_run("INTERNAL_ERROR", "FAILED", "Workload failed")
    outcome = jobs.wait_for_run(w, 1, timeout_s=60, poll_s=0)
    assert not outcome.ok and outcome.message == "Workload failed"


def test_wait_for_run_times_out_and_cancels(w):
    w.jobs.get_run.return_value = make_run("RUNNING")
    with pytest.raises(TimeoutError):
        jobs.wait_for_run(w, 1, timeout_s=-1, poll_s=0)
    w.jobs.cancel_run.assert_called_once_with(run_id=1)


def test_resolve_job_id_numeric_skips_api(w):
    assert jobs.resolve_job_id(w, "42") == 42
    w.jobs.list.assert_not_called()


def test_resolve_job_id_by_name(w):
    w.jobs.list.return_value = [ns(job_id=7)]
    assert jobs.resolve_job_id(w, "my_job") == 7


@pytest.mark.parametrize("listed", [[], [ns(job_id=1), ns(job_id=2)]], ids=["not_found", "ambiguous"])
def test_resolve_job_id_errors(w, listed):
    w.jobs.list.return_value = listed
    with pytest.raises(LookupError):
        jobs.resolve_job_id(w, "x")


def test_resolve_pipeline_by_name_and_id(w):
    w.pipelines.list_pipelines.return_value = [ns(name="etl", pipeline_id=PID)]
    assert resolve_pipeline_id(w, "etl") == PID
    assert resolve_pipeline_id(w, PID) == PID
