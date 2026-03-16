import os
from datetime import datetime, timedelta, timezone
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


def create_recording_with_detections(temp_dir, *, detection_count=1, outside_range=False):
    recording_path = temp_dir / "data" / f"recording-{uuid4().hex}.wav"
    recording_path.write_bytes(b"recording")

    clip_paths = []
    detection_ids = []

    started_at = datetime.utcnow()
    if outside_range:
        started_at = started_at - timedelta(days=3)
    ended_at = started_at + timedelta(seconds=27)

    recording = Recording(
        file_path=str(recording_path),
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=27.0,
        sample_rate=16000,
        channels=1,
        size_bytes=recording_path.stat().st_size,
        peak_amplitude=0.4,
        device_name="Test mic",
        has_bird_activity=detection_count > 0,
        bird_event_count=detection_count,
    )
    db.session.add(recording)
    db.session.flush()

    for index in range(detection_count):
        clip_path = temp_dir / "data" / f"clip-{uuid4().hex}-{index}.wav"
        clip_path.write_bytes(f"clip-{index}".encode("utf-8"))
        clip_paths.append(clip_path)
        detection = BirdDetection(
            recording_id=recording.id,
            started_at=started_at + timedelta(seconds=index * 9),
            ended_at=started_at + timedelta(seconds=(index * 9) + 3),
            confidence=0.81 - (index * 0.05),
            dominant_frequency_hz=0.0,
            source="birdnet",
            species_common_name="Robin",
            species_scientific_name="Erithacus rubecula",
            species_score=0.81 - (index * 0.05),
            clip_file_path=str(clip_path),
            clip_duration_seconds=3.0,
        )
        db.session.add(detection)
        db.session.flush()
        detection_ids.append(detection.id)

    db.session.commit()
    return {
        "recording_id": recording.id,
        "recording_path": recording_path,
        "clip_paths": clip_paths,
        "detection_ids": detection_ids,
        "started_at": started_at,
        "ended_at": ended_at,
    }


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

    with app.app_context():
        db.drop_all()
        db.create_all()
        created = create_recording_with_detections(temp_dir)
        recording_id = created["recording_id"]

    client = app.test_client()
    response = client.delete(f"/api/recordings/{recording_id}")

    assert response.status_code == 200
    assert not created["recording_path"].exists()
    assert not created["clip_paths"][0].exists()

    with app.app_context():
        assert Recording.query.count() == 0
        assert BirdDetection.query.count() == 0


def test_delete_detection_removes_only_one_clip_entry():
    app, temp_dir = build_test_app()

    with app.app_context():
        db.drop_all()
        db.create_all()
        created = create_recording_with_detections(temp_dir, detection_count=2)

    client = app.test_client()
    response = client.delete(f"/api/detections/{created['detection_ids'][0]}")

    assert response.status_code == 200
    assert not created["clip_paths"][0].exists()
    assert created["clip_paths"][1].exists()
    assert created["recording_path"].exists()

    with app.app_context():
        assert Recording.query.count() == 1
        recording = Recording.query.get(created["recording_id"])
        assert recording is not None
        assert recording.bird_event_count == 1
        assert recording.has_bird_activity is True
        remaining_detection_ids = [detection.id for detection in BirdDetection.query.order_by(BirdDetection.id.asc()).all()]
        assert remaining_detection_ids == [created["detection_ids"][1]]


def test_delete_detection_range_removes_only_visible_clip_entries():
    app, temp_dir = build_test_app()

    with app.app_context():
        db.drop_all()
        db.create_all()
        visible = create_recording_with_detections(temp_dir, detection_count=2)
        outside = create_recording_with_detections(temp_dir, detection_count=1, outside_range=True)

    client = app.test_client()
    response = client.post(
        "/api/detections/delete-range",
        json={
            "start": (visible["started_at"] - timedelta(minutes=1)).replace(tzinfo=timezone.utc).isoformat(),
            "end": (visible["ended_at"] + timedelta(minutes=1)).replace(tzinfo=timezone.utc).isoformat(),
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["deleted_detection_count"] == 2
    assert payload["affected_recording_count"] == 1
    assert not visible["clip_paths"][0].exists()
    assert not visible["clip_paths"][1].exists()
    assert outside["clip_paths"][0].exists()
    assert visible["recording_path"].exists()
    assert outside["recording_path"].exists()

    with app.app_context():
        visible_recording = Recording.query.get(visible["recording_id"])
        outside_recording = Recording.query.get(outside["recording_id"])
        assert visible_recording is not None
        assert outside_recording is not None
        assert visible_recording.bird_event_count == 0
        assert visible_recording.has_bird_activity is False
        assert outside_recording.bird_event_count == 1
        assert outside_recording.has_bird_activity is True
        remaining_detection_ids = [detection.id for detection in BirdDetection.query.order_by(BirdDetection.id.asc()).all()]
        assert remaining_detection_ids == outside["detection_ids"]
