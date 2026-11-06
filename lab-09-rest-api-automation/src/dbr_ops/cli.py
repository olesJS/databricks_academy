import argparse
import sys
import logging

from databricks.sdk.errors import DatabricksError

from dbr_ops.models import ClusterSpec
from dbr_ops.client import get_client, get_identity
from dbr_ops.clusters import create_cluster, delete_cluster


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dbrops", description="Databricks platform-management CLI")
    parser.add_argument("-p", "--profile", help="Databricks CLI profile")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("whoami", help="show authenticated profile")

    cluster = sub.add_parser("cluster", help="manage clusters")
    sub_cluster = cluster.add_subparsers(dest="action", required=True)

    create = sub_cluster.add_parser("create", help="create a cluster and wait until Running")
    create.add_argument("--name", required=True)
    create.add_argument("--workers", type=int, default=0, help="0 = single mode")
    create.add_argument("--node_type", help="default: auto-selected")

    delete = sub_cluster.add_parser("delete", help="permanently delete a cluster")
    delete.add_argument("--cluster_id", required=True)

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


def main(argv=None):
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    handlers = {
        "whoami": cmd_whoami,
        "cluster": cmd_cluster
    }

    try:
        return handlers[args.command](args)
    except DatabricksError as e:
        logging.getLogger(__name__).error("Databricks API error: %s", e)
        return 1


if __name__=="__main__":
    sys.exit(main())
