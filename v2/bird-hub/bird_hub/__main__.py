from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import BirdHubConfig
from .ingest import ingest_bundle_path
from .storage import BirdHubStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="bird-hub administration commands")
    subparsers = parser.add_subparsers(dest="command", required=True)

    token_parser = subparsers.add_parser("create-node-token", help="Create an ingest token for a node.")
    token_parser.add_argument("node_id")
    token_parser.add_argument("--label", default=None)

    ingest_parser = subparsers.add_parser("ingest-bundle", help="Ingest a node export zip from disk.")
    ingest_parser.add_argument("bundle_path")

    status_parser = subparsers.add_parser("status", help="Print current hub status JSON.")
    status_parser.add_argument("--pretty", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = BirdHubConfig.from_env()
    storage = BirdHubStorage(config)
    storage.initialize()

    if args.command == "create-node-token":
        token = storage.create_node_token(args.node_id, label=args.label)
        print(token)
        return 0

    if args.command == "ingest-bundle":
        result = ingest_bundle_path(config, storage, Path(args.bundle_path))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "status":
        payload = {
            "app": {
                "variant": config.app_variant,
                "commit": config.app_commit,
            },
            "counts": storage.get_hub_summary(),
        }
        if args.pretty:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(json.dumps(payload, sort_keys=True))
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
