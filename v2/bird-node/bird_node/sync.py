from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from .config import BirdNodeConfig
from .exporter import export_selected_records_archive
from .runtime_logging import get_application_logger
from .storage import BirdNodeStorage


def utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() + "Z"


class BirdNodeSyncManager:
    def __init__(self, config: BirdNodeConfig, storage: BirdNodeStorage, stop_event: threading.Event) -> None:
        self.config = config
        self.storage = storage
        self.stop_event = stop_event
        self.logger = get_application_logger()
        self.thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._last_attempt_at: datetime | None = None
        self._last_successful_sync_at: datetime | None = None
        self._last_error: str | None = None
        self._current_batch_id: int | None = None
        self._message: str = "Hub sync is not configured."

    def start(self) -> None:
        if not self.enabled:
            self._message = "Hub sync is disabled until BIRD_MONITOR_HUB_URL is configured."
            return
        if self.thread is not None:
            return
        self.thread = threading.Thread(
            target=self._run_loop,
            name="bird-node-sync",
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        if self.thread is None:
            return
        self.thread.join(timeout=10.0)

    @property
    def enabled(self) -> bool:
        return bool(self.config.hub_url)

    def run_once(self, *, force: bool = False) -> None:
        if not self.enabled:
            self._update_state(message="Hub sync is disabled until BIRD_MONITOR_HUB_URL is configured.")
            return

        now = datetime.utcnow()
        cycle_watermark = self.storage.get_unsynced_sync_watermark()
        batch = self.storage.get_next_sync_batch(now_utc=utc_iso(now) or "")
        if batch is None:
            summary = self.storage.get_sync_summary()
            if int(summary.get("queued_batch_count") or 0) > 0:
                self._update_state(
                    last_error=None,
                    message="A queued hub upload batch is waiting for its next retry window.",
                )
                return

            unsynced_detection_count = int(summary.get("unsynced_detection_count") or 0)
            unsynced_health_snapshot_count = int(summary.get("unsynced_health_snapshot_count") or 0)
            if unsynced_detection_count == 0 and unsynced_health_snapshot_count == 0:
                self._update_state(
                    last_error=None,
                    message="Hub sync is idle. No unsynced detections or health snapshots are queued.",
                )
                return

            next_regular_attempt_at = self._next_regular_attempt_at()
            if not force and next_regular_attempt_at is not None and now < next_regular_attempt_at:
                self._update_state(
                    last_error=None,
                    message=(
                        "Waiting until "
                        f"{utc_iso(next_regular_attempt_at)} for the next scheduled hub upload attempt."
                    ),
                )
                return

            batch = self._create_next_batch(
                max_detection_id=cycle_watermark["max_detection_id"],
                max_health_snapshot_id=cycle_watermark["max_health_snapshot_id"],
            )
            if batch is None:
                return

        while batch is not None and not self.stop_event.is_set():
            outcome = self._upload_batch(batch)
            if outcome == "failed":
                return

            batch = self.storage.get_next_sync_batch(now_utc=utc_iso(datetime.utcnow()) or "")
            if batch is None:
                batch = self._create_next_batch(
                    max_detection_id=cycle_watermark["max_detection_id"],
                    max_health_snapshot_id=cycle_watermark["max_health_snapshot_id"],
                )

    def status_payload(self) -> dict[str, object]:
        summary = self.storage.get_sync_summary()
        with self._state_lock:
            return {
                "enabled": self.enabled,
                "hub_url": self.config.hub_url,
                "last_attempt_at": utc_iso(self._last_attempt_at),
                "last_successful_sync_at": utc_iso(self._last_successful_sync_at) or summary.get("last_successful_sync_at"),
                "last_error": self._last_error if self._last_error is not None else summary.get("last_error"),
                "message": self._message,
                "current_batch_id": self._current_batch_id,
                "queued_batch_count": summary.get("queued_batch_count"),
                "failed_batch_count": summary.get("failed_batch_count"),
                "synced_batch_count": summary.get("synced_batch_count"),
                "unsynced_detection_count": summary.get("unsynced_detection_count"),
                "unsynced_health_snapshot_count": summary.get("unsynced_health_snapshot_count"),
                "last_batch_status": summary.get("last_batch_status"),
                "last_batch_created_at": summary.get("last_batch_created_at"),
                "regular_upload_interval_seconds": self.config.sync_interval_seconds,
                "retry_interval_seconds": self.config.sync_retry_base_seconds,
                "next_regular_attempt_at": utc_iso(self._next_regular_attempt_at()),
            }

    def _run_loop(self) -> None:
        self.logger.info(
            "Starting hub sync manager hub_url=%s regular_interval_seconds=%.1f retry_interval_seconds=%.1f",
            self.config.hub_url,
            self.config.sync_interval_seconds,
            self.config.sync_retry_base_seconds,
        )
        while not self.stop_event.is_set():
            try:
                self.run_once()
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                self.logger.exception("Unexpected hub sync failure.")
                self._update_state(last_error=str(exc), message=f"Hub sync failed unexpectedly: {exc}")
            if self.stop_event.wait(self._loop_wait_seconds()):
                break

    def _create_next_batch(
        self,
        *,
        max_detection_id: int | None = None,
        max_health_snapshot_id: int | None = None,
    ) -> dict[str, Any] | None:
        detection_ids = self.storage.list_unsynced_detection_ids(
            limit=self.config.sync_max_events_per_bundle,
            max_id=max_detection_id if (max_detection_id or 0) > 0 else None,
        )
        snapshot_ids = self.storage.list_unsynced_health_snapshot_ids(
            limit=self.config.sync_max_health_snapshots_per_bundle,
            max_id=max_health_snapshot_id if (max_health_snapshot_id or 0) > 0 else None,
        )
        if not detection_ids and not snapshot_ids:
            return None

        created_at = utc_iso(datetime.utcnow()) or ""
        temporary_path = str(
            (self.config.sync_queue_dir / f"{self.config.node_id}-batch-{created_at.replace(':', '').replace('.', '')}.zip").resolve()
        )
        batch_id = self.storage.create_sync_batch(
            bundle_path=temporary_path,
            hub_url=self.config.hub_url or "",
            detection_ids=detection_ids,
            health_snapshot_ids=snapshot_ids,
            created_at=created_at,
        )
        batch_path = (self.config.sync_queue_dir / f"batch-{batch_id:06d}.zip").resolve()
        try:
            export_selected_records_archive(
                self.config,
                detection_ids=detection_ids,
                explicit_snapshot_ids=snapshot_ids,
                output_path=batch_path,
            )
        except Exception as exc:
            self.storage.fail_sync_batch_build(batch_id, error_message=str(exc), updated_at=utc_iso(datetime.utcnow()) or "")
            self.logger.exception("Failed to build sync batch %s.", batch_id)
            self._update_state(last_error=str(exc), message=f"Failed to build sync batch {batch_id}.")
            return None

        self.storage.mark_sync_batch_pending(batch_id, bundle_path=str(batch_path), updated_at=utc_iso(datetime.utcnow()) or "")
        self.logger.info(
            "Prepared sync batch id=%s detections=%s health_snapshots=%s path=%s",
            batch_id,
            len(detection_ids),
            len(snapshot_ids),
            batch_path,
        )
        return self.storage.get_next_sync_batch(now_utc=utc_iso(datetime.utcnow()) or "")

    def _upload_batch(self, batch: dict[str, Any]) -> str:
        batch_id = int(batch["id"])
        batch_path = Path(str(batch["bundle_path"])).resolve()
        if not batch_path.exists():
            self.storage.abandon_sync_batch(
                batch_id,
                error_message=f"Sync batch file is missing: {batch_path}",
                updated_at=utc_iso(datetime.utcnow()) or "",
            )
            self._update_state(
                current_batch_id=batch_id,
                last_error=f"Sync batch file is missing: {batch_path}",
                message=f"Batch {batch_id} was abandoned because its bundle file no longer exists.",
            )
            return "abandoned"

        attempted_at = datetime.utcnow()
        self.storage.mark_sync_batch_uploading(batch_id, attempted_at=utc_iso(attempted_at) or "")
        self._update_state(current_batch_id=batch_id, last_attempt_at=attempted_at, message=f"Uploading sync batch {batch_id}.")

        url = f"{str(self.config.hub_url).rstrip('/')}/api/v1/ingest/bundle"
        headers: dict[str, str] = {}
        if self.config.hub_token:
            headers["Authorization"] = f"Bearer {self.config.hub_token}"

        try:
            with batch_path.open("rb") as handle:
                response = requests.post(
                    url,
                    headers=headers,
                    files={"bundle": (batch_path.name, handle, "application/zip")},
                    timeout=(10, 120),
                )
            if response.status_code >= 400:
                message = response.text.strip() or f"Hub returned HTTP {response.status_code}"
                raise RuntimeError(f"Hub upload failed with HTTP {response.status_code}: {message}")

            payload = response.json()
            if not bool(payload.get("acknowledged")):
                raise RuntimeError("Hub upload did not return an explicit acknowledgment.")
            synced_at = datetime.utcnow()
            self.storage.mark_sync_batch_synced(
                batch_id,
                synced_at=utc_iso(synced_at) or "",
                response_payload=payload,
            )
            cleanup_summary = self.storage.purge_uploaded_records(
                detection_ids=[int(item) for item in list(batch.get("detection_ids") or [])],
                health_snapshot_ids=[int(item) for item in list(batch.get("health_snapshot_ids") or [])],
            )
            try:
                batch_path.unlink(missing_ok=True)
            except OSError:
                pass
            self.logger.info(
                "Uploaded sync batch id=%s detections=%s health_snapshots=%s acknowledged=%s deleted_detections=%s deleted_clips=%s deleted_health_snapshots=%s",
                batch_id,
                batch.get("detection_count"),
                batch.get("health_snapshot_count"),
                payload.get("acknowledged"),
                cleanup_summary.get("deleted_detection_count"),
                cleanup_summary.get("deleted_clip_count"),
                cleanup_summary.get("deleted_health_snapshot_count"),
            )
            self._update_state(
                current_batch_id=None,
                last_attempt_at=attempted_at,
                last_successful_sync_at=synced_at,
                last_error=None,
                message=f"Uploaded sync batch {batch_id} successfully and deleted the acknowledged local payload.",
            )
            return "synced"
        except Exception as exc:
            backoff_seconds = max(self.config.sync_retry_base_seconds, 5.0)
            next_retry_at = attempted_at + timedelta(seconds=backoff_seconds)
            self.storage.mark_sync_batch_failed(
                batch_id,
                error_message=str(exc),
                attempted_at=utc_iso(attempted_at) or "",
                next_retry_at=utc_iso(next_retry_at),
            )
            self.logger.warning(
                "Hub upload failed for batch id=%s. Retrying after %.1fs. error=%s",
                batch_id,
                backoff_seconds,
                exc,
            )
            self._update_state(
                current_batch_id=None,
                last_attempt_at=attempted_at,
                last_error=str(exc),
                message=f"Sync batch {batch_id} failed and will retry later.",
            )
            return "failed"

    def _loop_wait_seconds(self) -> float:
        return max(min(self.config.sync_interval_seconds, self.config.sync_retry_base_seconds, 300.0), 5.0)

    def _next_regular_attempt_at(self) -> datetime | None:
        last_created_at_raw = self.storage.get_last_sync_batch_created_at()
        if not last_created_at_raw:
            return None
        try:
            normalized = last_created_at_raw[:-1] if last_created_at_raw.endswith("Z") else last_created_at_raw
            last_created_at = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return last_created_at + timedelta(seconds=max(self.config.sync_interval_seconds, 0.0))

    def _update_state(
        self,
        *,
        current_batch_id: int | None | object = ...,
        last_attempt_at: datetime | None | object = ...,
        last_successful_sync_at: datetime | None | object = ...,
        last_error: str | None | object = ...,
        message: str | None = None,
    ) -> None:
        with self._state_lock:
            if current_batch_id is not ...:
                self._current_batch_id = current_batch_id  # type: ignore[assignment]
            if last_attempt_at is not ...:
                self._last_attempt_at = last_attempt_at  # type: ignore[assignment]
            if last_successful_sync_at is not ...:
                self._last_successful_sync_at = last_successful_sync_at  # type: ignore[assignment]
            if last_error is not ...:
                self._last_error = last_error  # type: ignore[assignment]
            if message is not None:
                self._message = message
