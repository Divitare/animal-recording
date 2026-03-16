import os
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from bird_monitor.app import create_app
from bird_monitor.extensions import db
from bird_monitor.models import BirdDetection, Recording

TEST_ROOT = Path(__file__).resolve().parents[1] / ".tmp" / "test-api"
TEST_ROOT.mkdir(parents=True, exist_ok=True)


def build_test_app():
    temp_dir = TEST_ROOT / f"bird-monitor-test-api-{uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    data_dir = temp_dir / "data"
    logs_dir = temp_dir / "logs"
    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    os.environ["BIRD_MONITOR_DISABLE_RECORDER"] = "true"
    os.environ["BIRD_MONITOR_DATA_DIR"] = str(data_dir)
    os.environ["BIRD_MONITOR_LOG_DIR"] = str(logs_dir)
    os.environ["BIRD_MONITOR_DATABASE_URI"] = f"sqlite:///{(temp_dir / 'bird-monitor.db').as_posix()}"
    os.environ["BIRD_MONITOR_SECRET_KEY"] = "test"

    app = create_app()
    app.config.update(TESTING=True)
    return app, temp_dir


def test_download_logs_returns_zip_file():
    app, _ = build_test_app()

    with app.app_context():
        birdnet_log = Path(app.config["BIRDNET_LOG_FILE"])
        app_log = Path(app.config["APP_LOG_FILE"])
        birdnet_log.write_text("birdnet-log\n", encoding="utf-8")
        app_log.write_text("app-log\n", encoding="utf-8")

    client = app.test_client()
    response = client.get("/api/birdnet/logs/download")

    assert response.status_code == 200
    assert response.mimetype == "application/zip"
    assert "bird-monitor-logs-" in response.headers.get("content-disposition", "")


def test_delete_recording_removes_recording_and_clip_files():
    app, temp_dir = build_test_app()
    recording_path = temp_dir / "data" / "recording.wav"
    clip_path = temp_dir / "data" / "clip.wav"
    recording_path.write_bytes(b"recording")
    clip_path.write_bytes(b"clip")

    with app.app_context():
        db.drop_all()
        db.create_all()
        started_at = datetime.utcnow()
        ended_at = started_at + timedelta(seconds=9)
        recording = Recording(
            file_path=str(recording_path),
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=9.0,
            sample_rate=16000,
            channels=1,
            size_bytes=recording_path.stat().st_size,
            peak_amplitude=0.4,
            device_name="Test mic",
            has_bird_activity=True,
            bird_event_count=1,
        )
        db.session.add(recording)
        db.session.flush()
        detection = BirdDetection(
            recording_id=recording.id,
            started_at=started_at,
            ended_at=started_at + timedelta(seconds=3),
            confidence=0.81,
            dominant_frequency_hz=0.0,
            source="birdnet",
            species_common_name="Robin",
            species_scientific_name="Erithacus rubecula",
            species_score=0.81,
            clip_file_path=str(clip_path),
            clip_duration_seconds=3.0,
        )
        db.session.add(detection)
        db.session.commit()
        recording_id = recording.id

    client = app.test_client()
    response = client.delete(f"/api/recordings/{recording_id}")

    assert response.status_code == 200
    assert not recording_path.exists()
    assert not clip_path.exists()

    with app.app_context():
        assert Recording.query.count() == 0
        assert BirdDetection.query.count() == 0
