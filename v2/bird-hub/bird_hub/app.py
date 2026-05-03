from __future__ import annotations

from pathlib import Path
import shutil
import time

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, url_for

from .config import BirdHubConfig
from .ingest import IngestError, ingest_bundle_file
from .storage import BirdHubStorage


def _extract_bearer_token() -> str | None:
    auth_header = request.headers.get("Authorization", "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip() or None
    return None


def _coerce_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _coerce_int(value: str | None, default: int | None = None) -> int | None:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _display_utc(value: str | None) -> str:
    if not value:
        return "-"
    return value.replace("T", " ").replace("Z", " UTC")


def _format_bytes(value: object) -> str:
    if value in (None, "", 0):
        return "-"
    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    return f"{size:.1f} {units[unit_index]}"


def _resolve_ui_path(value: str | None, *, kind: str) -> Path:
    candidate = Path((value or "").strip()).expanduser()
    if not str(candidate).strip():
        raise ValueError(f"Please enter a {kind} path.")
    if not candidate.is_absolute():
        raise ValueError(f"The {kind} path must be absolute.")
    return candidate.resolve()


def _ensure_writable_directory(path: Path, *, label: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    probe_path = path / f".bird-hub-write-test-{time.time_ns()}"
    try:
        probe_path.write_text("ok", encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"The {label} is not writable: {path} ({exc})") from exc
    finally:
        try:
            probe_path.unlink(missing_ok=True)
        except OSError:
            pass


def _ensure_writable_file_parent(path: Path, *, label: str) -> None:
    _ensure_writable_directory(path.parent, label=label)


def _copy_tree_files(source_root: Path, destination_root: Path) -> int:
    if source_root == destination_root or not source_root.exists():
        return 0
    files = [path for path in source_root.rglob("*") if path.is_file()]
    copied_destinations: list[Path] = []
    for source_path in files:
        relative_path = source_path.relative_to(source_root)
        destination_path = destination_root / relative_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        if destination_path.exists():
            raise ValueError(f"Destination file already exists: {destination_path}")
        shutil.copy2(source_path, destination_path)
        copied_destinations.append(destination_path)
    try:
        for source_path in files:
            source_path.unlink(missing_ok=True)
    except OSError as exc:
        raise ValueError(f"Copied files to {destination_root}, but could not clean up the old location: {exc}") from exc
    for directory_path in sorted((path for path in source_root.rglob("*") if path.is_dir()), key=lambda item: len(item.parts), reverse=True):
        try:
            directory_path.rmdir()
        except OSError:
            continue
    return len(copied_destinations)


def _copy_database_files(source_database: Path, destination_database: Path) -> int:
    if source_database == destination_database or not source_database.exists():
        return 0
    source_paths = [
        source_database,
        source_database.with_name(f"{source_database.name}-wal"),
        source_database.with_name(f"{source_database.name}-shm"),
    ]
    destination_paths = [
        destination_database,
        destination_database.with_name(f"{destination_database.name}-wal"),
        destination_database.with_name(f"{destination_database.name}-shm"),
    ]
    copied_pairs: list[tuple[Path, Path]] = []
    for source_path, destination_path in zip(source_paths, destination_paths):
        if not source_path.exists():
            continue
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        if destination_path.exists():
            raise ValueError(f"Destination file already exists: {destination_path}")
        shutil.copy2(source_path, destination_path)
        copied_pairs.append((source_path, destination_path))
    try:
        for source_path, _destination_path in copied_pairs:
            source_path.unlink(missing_ok=True)
    except OSError as exc:
        raise ValueError(
            f"Copied the database to {destination_database}, but could not clean up the old database files: {exc}"
        ) from exc
    return len(copied_pairs)


def create_app() -> Flask:
    config = BirdHubConfig.from_env()
    config.ensure_directories()
    storage = BirdHubStorage(config)
    storage.initialize()

    app = Flask(__name__)
    app.config.update(
        SECRET_KEY=config.secret_key,
        APP_VARIANT=config.app_variant,
        APP_COMMIT=config.app_commit,
        MAX_CONTENT_LENGTH=config.max_bundle_bytes,
    )
    app.storage = storage  # type: ignore[attr-defined]
    app.hub_config = config  # type: ignore[attr-defined]

    @app.context_processor
    def inject_globals() -> dict[str, object]:
        return {
            "app_variant": config.app_variant,
            "app_commit": config.app_commit,
        }

    @app.template_filter("utc_display")
    def utc_display_filter(value: str | None) -> str:
        return _display_utc(value)

    @app.template_filter("filesize")
    def filesize_filter(value: object) -> str:
        return _format_bytes(value)

    @app.get("/")
    def dashboard() -> str:
        summary = storage.get_hub_summary()
        nodes = storage.list_nodes()[:8]
        batches = storage.list_ingest_batches(limit=10)
        species_stats = storage.list_species_stats()[:12]
        return render_template(
            "dashboard.html",
            summary=summary,
            nodes=nodes,
            batches=batches,
            species_stats=species_stats,
        )

    @app.post("/upload-bundle")
    def upload_bundle_page() -> str:
        bundle = request.files.get("bundle")
        if bundle is None or not bundle.filename:
            flash("Please choose a node export zip before uploading.", "error")
            return redirect(url_for("dashboard"))
        try:
            result = ingest_bundle_file(config, storage, bundle)
        except IngestError as exc:
            flash(str(exc), "error")
        except Exception as exc:  # pragma: no cover
            flash(f"Bundle upload failed: {exc}", "error")
        else:
            flash(
                f"Ingested {result['processed_event_count']} event(s), "
                f"{result['processed_snapshot_count']} health snapshot(s), "
                f"and {result['processed_clip_count']} clip(s) from {result['node_id']}.",
                "success",
            )
        return redirect(url_for("dashboard"))

    @app.get("/settings/storage")
    def storage_settings_page() -> str:
        return render_template(
            "storage_settings.html",
            storage_settings={
                "database_path": str(config.database_path),
                "clip_dir": str(config.clip_dir),
                "upload_dir": str(config.upload_dir),
                "settings_path": str(config.settings_path),
            },
        )

    @app.post("/settings/storage")
    def save_storage_settings_page() -> str:
        try:
            new_database_path = _resolve_ui_path(request.form.get("database_path"), kind="database file")
            new_clip_dir = _resolve_ui_path(request.form.get("clip_dir"), kind="clip storage directory")
            new_upload_dir = _resolve_ui_path(request.form.get("upload_dir"), kind="upload directory")
            move_database = request.form.get("move_database") == "on"
            move_clips = request.form.get("move_clips") == "on"
            move_uploads = request.form.get("move_uploads") == "on"

            _ensure_writable_file_parent(new_database_path, label="database file location")
            _ensure_writable_directory(new_clip_dir, label="clip storage directory")
            _ensure_writable_directory(new_upload_dir, label="upload directory")

            old_database_path = config.database_path
            old_clip_dir = config.clip_dir
            old_upload_dir = config.upload_dir

            moved_database_files = 0
            moved_clip_files = 0
            moved_upload_files = 0

            if move_database:
                moved_database_files = _copy_database_files(old_database_path, new_database_path)
            if move_clips:
                moved_clip_files = _copy_tree_files(old_clip_dir, new_clip_dir)
            if move_uploads:
                moved_upload_files = _copy_tree_files(old_upload_dir, new_upload_dir)

            config.database_path = new_database_path
            config.clip_dir = new_clip_dir
            config.upload_dir = new_upload_dir
            config.ensure_directories()
            storage.initialize()
            config.save_runtime_settings()
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("storage_settings_page"))
        except Exception as exc:  # pragma: no cover
            flash(f"Could not save storage settings: {exc}", "error")
            return redirect(url_for("storage_settings_page"))

        flash(
            "Storage settings saved."
            f" Database files moved: {moved_database_files}. "
            f"Clip files moved: {moved_clip_files}. "
            f"Upload files moved: {moved_upload_files}.",
            "success",
        )
        return redirect(url_for("storage_settings_page"))

    @app.get("/nodes")
    def nodes_page() -> str:
        return render_template("nodes.html", nodes=storage.list_nodes())

    @app.get("/nodes/<node_id>")
    def node_detail_page(node_id: str) -> str:
        node = storage.get_node(node_id)
        if node is None:
            abort(404)
        return render_template(
            "node_detail.html",
            node=node,
            recent_events=storage.list_events(node_id=node_id, limit=50),
            health_snapshots=storage.list_health_snapshots(node_id=node_id, limit=12),
            species_stats=storage.list_species_stats(node_id=node_id),
        )

    @app.get("/events")
    def events_page() -> str:
        node_id = request.args.get("node_id") or None
        since_utc = request.args.get("since_utc") or None
        until_utc = request.args.get("until_utc") or None
        species = request.args.get("species") or None
        min_confidence = _coerce_float(request.args.get("min_confidence"))
        limit = _coerce_int(request.args.get("limit"), default=config.default_event_limit)
        events = storage.list_events(
            node_id=node_id,
            since_utc=since_utc,
            until_utc=until_utc,
            species=species,
            min_confidence=min_confidence,
            limit=limit,
        )
        return render_template(
            "events.html",
            events=events,
            nodes=storage.list_nodes(),
            filters={
                "node_id": node_id,
                "since_utc": since_utc,
                "until_utc": until_utc,
                "species": species,
                "min_confidence": request.args.get("min_confidence", ""),
                "limit": limit,
            },
        )

    @app.get("/events/<event_id>")
    def event_detail_page(event_id: str) -> str:
        event = storage.get_event(event_id)
        if event is None:
            abort(404)
        return render_template("event_detail.html", event=event)

    def _status_payload() -> dict[str, object]:
        return {
            "app": {
                "variant": config.app_variant,
                "commit": config.app_commit,
            },
            "hub": {
                "settings_path": str(config.settings_path),
                "database_path": str(config.database_path),
                "clip_dir": str(config.clip_dir),
                "upload_dir": str(config.upload_dir),
                "allow_unauthenticated_ingest": config.allow_unauthenticated_ingest,
            },
            "counts": storage.get_hub_summary(),
        }

    @app.get("/api/v1/status")
    @app.get("/api/status")
    def api_status() -> object:
        return jsonify(
            _status_payload()
        )

    @app.post("/api/v1/ingest/bundle")
    def api_ingest_bundle() -> object:
        bundle = request.files.get("bundle") or request.files.get("file")
        if bundle is None or not bundle.filename:
            return jsonify({"error": "Upload a zip file in the 'bundle' form field."}), 400

        token_count = storage.active_token_count()
        authorized_node_id = None
        if token_count > 0:
            token = _extract_bearer_token()
            authorized_node_id = storage.authenticate_token(token or "")
            if authorized_node_id is None:
                return jsonify({"error": "A valid Bearer token is required for ingest."}), 401
        elif not config.allow_unauthenticated_ingest:
            return jsonify({"error": "Unauthenticated ingest is disabled on this hub."}), 403

        try:
            result = ingest_bundle_file(config, storage, bundle, authorized_node_id=authorized_node_id)
        except IngestError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:  # pragma: no cover
            return jsonify({"error": str(exc)}), 500
        return jsonify(result)

    @app.get("/api/v1/nodes")
    def api_nodes() -> object:
        return jsonify({"nodes": storage.list_nodes()})

    @app.get("/api/v1/nodes/<node_id>")
    def api_node_detail(node_id: str) -> object:
        node = storage.get_node(node_id)
        if node is None:
            return jsonify({"error": "Node not found."}), 404
        return jsonify(
            {
                "node": node,
                "recent_events": storage.list_events(node_id=node_id, limit=50),
                "recent_health_snapshots": storage.list_health_snapshots(node_id=node_id, limit=12),
                "species_stats": storage.list_species_stats(node_id=node_id),
            }
        )

    @app.get("/api/v1/events")
    def api_events() -> object:
        events = storage.list_events(
            node_id=request.args.get("node_id") or None,
            since_utc=request.args.get("since_utc") or None,
            until_utc=request.args.get("until_utc") or None,
            species=request.args.get("species") or None,
            min_confidence=_coerce_float(request.args.get("min_confidence")),
            limit=_coerce_int(request.args.get("limit"), default=config.default_event_limit),
        )
        return jsonify({"events": events, "count": len(events)})

    @app.get("/api/v1/events/<event_id>")
    def api_event_detail(event_id: str) -> object:
        event = storage.get_event(event_id)
        if event is None:
            return jsonify({"error": "Event not found."}), 404
        return jsonify({"event": event})

    @app.get("/api/v1/events/<event_id>/clip")
    def api_event_clip(event_id: str) -> object:
        event = storage.get_event(event_id)
        if event is None or event.get("clip") is None:
            return jsonify({"error": "Clip not found."}), 404
        clip = event["clip"]
        clip_path = storage.clip_abspath(str(clip["storage_path"]))
        if not clip_path.exists():
            return jsonify({"error": "Clip file is missing from disk."}), 404
        return send_file(
            clip_path,
            mimetype="audio/wav",
            as_attachment=False,
            download_name=Path(str(clip["storage_path"])).name,
        )

    @app.get("/api/v1/species/stats")
    def api_species_stats() -> object:
        return jsonify(
            {
                "species_stats": storage.list_species_stats(
                    node_id=request.args.get("node_id") or None,
                    since_utc=request.args.get("since_utc") or None,
                    until_utc=request.args.get("until_utc") or None,
                )
            }
        )

    @app.get("/api/v1/health-snapshots")
    def api_health_snapshots() -> object:
        snapshots = storage.list_health_snapshots(
            node_id=request.args.get("node_id") or None,
            since_utc=request.args.get("since_utc") or None,
            until_utc=request.args.get("until_utc") or None,
            limit=_coerce_int(request.args.get("limit"), default=50),
        )
        return jsonify({"health_snapshots": snapshots, "count": len(snapshots)})

    @app.get("/api/v1/ingest/batches")
    def api_ingest_batches() -> object:
        batches = storage.list_ingest_batches(limit=_coerce_int(request.args.get("limit"), default=50))
        return jsonify({"ingest_batches": batches, "count": len(batches)})

    return app
