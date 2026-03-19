from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path

from .config import load_config
from .exporter import export_events_archive
from .runtime_logging import configure_logging, get_application_logger
from .service import BirdNodeService
from .storage import BirdNodeStorage
from .sync import BirdNodeSyncManager


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bird-node", description="Headless BirdNET field node runtime.")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("run", help="Run the continuous bird-node capture and detection service.")

    export_parser = subparsers.add_parser("export-events", help="Export detected bird events, clips, and health snapshots.")
    export_parser.add_argument("--output", help="Path to the export zip file.")
    export_parser.add_argument("--since-hours", type=float, default=24.0, help="How many hours back to export when --since-utc is not set.")
    export_parser.add_argument("--since-utc", help="UTC start timestamp, for example 2026-03-17T00:00:00Z.")
    export_parser.add_argument("--until-utc", help="UTC end timestamp, for example 2026-03-17T23:59:59Z.")

    subparsers.add_parser("sync-now", help="Run one immediate sync attempt against the configured bird-hub.")

    return parser


def run_service() -> None:
    config = load_config()
    log_paths = configure_logging(config.log_dir)
    logger = get_application_logger()
    logger.info("bird-node runtime logging ready app_log=%s birdnet_log=%s", log_paths["app_log_file"], log_paths["birdnet_log_file"])

    service = BirdNodeService(config)

    def handle_signal(signum, _frame) -> None:
        logger.info("Received signal %s. Stopping bird-node.", signum)
        service.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    service.run_forever()


def run_export(args: argparse.Namespace) -> None:
    config = load_config()
    log_paths = configure_logging(config.log_dir)
    logger = get_application_logger()
    logger.info("bird-node export requested app_log=%s birdnet_log=%s", log_paths["app_log_file"], log_paths["birdnet_log_file"])

    output_path = Path(args.output).expanduser().resolve() if args.output else None
    archive_path = export_events_archive(
        config,
        output_path=output_path,
        since_hours=float(args.since_hours),
        since_utc=args.since_utc,
        until_utc=args.until_utc,
    )
    print(archive_path)


def run_sync_now() -> None:
    config = load_config()
    log_paths = configure_logging(config.log_dir)
    logger = get_application_logger()
    logger.info("bird-node manual sync requested app_log=%s birdnet_log=%s", log_paths["app_log_file"], log_paths["birdnet_log_file"])

    storage = BirdNodeStorage(config.database_path, config.status_file)
    storage.initialize()
    sync_manager = BirdNodeSyncManager(config, storage, stop_event=threading.Event())
    try:
        sync_manager.run_once()
        print(json.dumps(sync_manager.status_payload(), indent=2, sort_keys=True))
    finally:
        sync_manager.stop()


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    command = args.command or "run"

    if command == "run":
        run_service()
        return
    if command == "export-events":
        run_export(args)
        return
    if command == "sync-now":
        run_sync_now()
        return

    parser.error(f"Unknown command: {command}")


if __name__ == "__main__":
    main(sys.argv[1:])
