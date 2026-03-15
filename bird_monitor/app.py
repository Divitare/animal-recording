from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, render_template, url_for

from .api import api_bp
from .database import ensure_schema
from .extensions import db
from .models import RecorderSettings
from .services import start_background_services


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def create_app() -> Flask:
    load_dotenv()

    package_root = Path(__file__).resolve().parent.parent
    data_dir = Path(os.getenv("BIRD_MONITOR_DATA_DIR", str(package_root / "data"))).resolve()
    recordings_dir = data_dir / "recordings"
    exports_dir = data_dir / "exports"
    clips_dir = data_dir / "clips"

    data_dir.mkdir(parents=True, exist_ok=True)
    recordings_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.update(
        SECRET_KEY=os.getenv("BIRD_MONITOR_SECRET_KEY", "bird-monitor-dev-key"),
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{(data_dir / 'bird_monitor.db').as_posix()}",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        DATA_DIR=str(data_dir),
        RECORDINGS_DIR=str(recordings_dir),
        EXPORTS_DIR=str(exports_dir),
        CLIPS_DIR=str(clips_dir),
        HOST=os.getenv("BIRD_MONITOR_HOST", "0.0.0.0"),
        PORT=int(os.getenv("BIRD_MONITOR_PORT", "8080")),
        DISABLE_BACKGROUND_RECORDER=_env_flag("BIRD_MONITOR_DISABLE_RECORDER", False),
    )

    db.init_app(app)
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.context_processor
    def asset_helpers():
        def asset_url(filename: str) -> str:
            asset_path = Path(app.static_folder or "") / filename
            version = int(asset_path.stat().st_mtime) if asset_path.exists() else 0
            return url_for("static", filename=filename, v=version)

        return {"asset_url": asset_url}

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/settings")
    def settings_page():
        return render_template("settings.html")

    with app.app_context():
        db.create_all()
        ensure_schema()
        RecorderSettings.get_or_create()

    start_background_services(app)
    return app
