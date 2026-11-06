import argparse
import sys

from dbr_ops.client import get_client, get_identity


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dbrops", description="Databricks platform-management CLI")
    parser.add_argument("--profile", help="Databricks CLI profile")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("whoami", help="show authenticated profile")

    return parser


def cmd_whoami(args):
    w = get_client(args.profile)
    identity = get_identity(w)
    print("Authenticated as:")
    print(f"User: {identity.user_name}")
    print(f"Name: {identity.display_name}")
    print(f"Workspace: {identity.host}")
    return 0


def main(argv=None):
    args = build_parser().parse_args(argv)
    handlers = {"whoami": cmd_whoami}

    return handlers[args.command](args)


if __name__=="__main__":
    sys.exit(main())
