import argparse
import sys
import logging

from databricks.sdk.errors import DatabricksError

from dbr_ops.models import ClusterSpec
from dbr_ops.client import get_client, get_identity
from dbr_ops.clusters import create_cluster, delete_cluster
from dbr_ops.jobs import run_job, run_notebook, get_run_status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dbrops", description="Databricks platform-management CLI")
    parser.add_argument("-p", "--profile", help="Databricks CLI profile")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    # whoami
    sub.add_parser("whoami", help="show authenticated profile")

    # cluster
    cluster = sub.add_parser("cluster", help="manage clusters")
    sub_cluster = cluster.add_subparsers(dest="action", required=True)

    create = sub_cluster.add_parser("create", help="create a cluster and wait until Running")
    create.add_argument("--name", required=True)
    create.add_argument("--workers", type=int, default=0, help="0 = single mode")
    create.add_argument("--node_type", help="default: auto-selected")

    delete = sub_cluster.add_parser("delete", help="permanently delete a cluster")
    delete.add_argument("--cluster_id", required=True)

    # job
    job = sub.add_parser("job", help="run jobs")
    sub_job = job.add_subparsers(dest="action", required=True)
    job_run = sub_job.add_parser("run", help="trigger a job and wait for the result")
    job_run.add_argument("job", help="job name or id")
    job_run.add_argument("--timeout", type=int, default=1800, help="seconds")

    notebook = sub.add_parser("notebook", help="run notebooks")
    sub_nb = notebook.add_subparsers(dest="action", required=True)
    nb_run = sub_nb.add_parser("run", help="run a notebook once and wait for the result")
    nb_run.add_argument("path", help="workspace path of the notebook")
    nb_run.add_argument("--cluster-id", help="existing cluster (default: serverless)")
    nb_run.add_argument("--timeout", type=int, default=1800, help="seconds")

    status = sub.add_parser("status", help="show the status of a run")
    status.add_argument("run_id", type=int)

    return parser


def setup_logging(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr
    )
    logging.getLogger("databricks.sdk").setLevel(logging.DEBUG if verbose else logging.WARNING)


def cmd_whoami(args):
    w = get_client(args.profile)
    identity = get_identity(w)
    print("Authenticated as:")
    print(f"User: {identity.user_name}")
    print(f"Name: {identity.display_name}")
    print(f"Workspace: {identity.host}")
    return 0


def cmd_cluster(args):
    w = get_client(args.profile)
    if args.action == "create":
        spec = ClusterSpec(
            name=args.name,
            num_workers=args.workers,
            node_type_id=args.node_type
        )
        cluster_id = create_cluster(w, spec)
        print(cluster_id)
    elif args.action == "delete":
        delete_cluster(w, args.cluster_id)
    return 0


def print_outcome(o):
    print(f"Run {o.run_id}: {o.life_cycle_state} / {o.result_state or '-'}")
    if o.message:
        print(f"Message: {o.message}")
    print(f"URL: {o.url}")


def cmd_job(args):
    outcome = run_job(get_client(args.profile), args.job, args.timeout)
    print_outcome(outcome)
    return 0 if outcome.ok else 1


def cmd_notebook(args):
    outcome = run_notebook(get_client(args.profile), args.path, args.cluster_id, args.timeout)
    print_outcome(outcome)
    return 0 if outcome.ok else 1


def cmd_status(args):
    outcome = get_run_status(get_client(args.profile), args.run_id)
    print_outcome(outcome)
    return 1 if outcome.finished and not outcome.ok else 0


def main(argv=None):
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    handlers = {
        "whoami": cmd_whoami,
        "cluster": cmd_cluster,
        "job": cmd_job,
        "notebook": cmd_notebook,
        "status": cmd_status
    }

    try:
        return handlers[args.command](args)
    except (DatabricksError, TimeoutError, LookupError) as e:
        logging.getLogger(__name__).error("%s", e)
        return 1


if __name__=="__main__":
    sys.exit(main())
