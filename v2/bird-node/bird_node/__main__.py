from __future__ import annotations

import signal

from .config import load_config
from .runtime_logging import configure_logging, get_application_logger
from .service import BirdNodeService


def main() -> None:
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


if __name__ == "__main__":
    main()
