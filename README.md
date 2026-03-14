# Bird Monitor

Bird Monitor is a Flask-based USB microphone recorder for bird sound monitoring. It stores audio files, marks likely bird activity in a timeline, lets you define recurring recording schedules, and can export a selected time span as a zip archive.

## What it does

- Records from a USB microphone during predefined schedule windows
- Lets you manually start and stop recording from the browser
- Shows the live microphone waveform and current recorder activity
- Stores each recording segment as a WAV file and tracks metadata in SQLite
- Detects likely bird activity inside each segment and marks those moments in the timeline
- Can analyze each saved recording with BirdNET and show species detections with timestamps in the timeline
- Shows past recordings in a continuous, zoomable web timeline
- Starts the dashboard on the last six hours and lets you zoom the timeline with the mouse wheel
- Lets you download every recording that overlaps a selected time span
- Lets you configure the microphone, BirdNET location, and segment length from the `/settings` page

## Species detection

Exact species detection is possible, but not perfectly reliable. In practice it usually needs a dedicated bird-classification model such as BirdNET plus a good microphone, clean audio, and regional context. This project can analyze each saved recording with BirdNET after capture and can use the configured latitude, longitude, and recording date to narrow down likely species for that region and season.
The Linux installer attempts to install the BirdNET runtime automatically and verifies it during setup. For local development, install `birdnetlib`, `librosa`, and either `tflite-runtime` or `tensorflow` if you want species labels during testing.

## Local development

1. Create a virtual environment: `python -m venv .venv`
2. Activate it and install dependencies: `pip install -r requirements-dev.txt`
3. Copy `.env.example` to `.env` and adjust values if needed
4. Run the app: `python -m bird_monitor`
5. Open `http://127.0.0.1:8080`

## Linux installation

Use `install.sh` on the target Linux machine:

```bash
chmod +x install.sh
./install.sh
```

Single-line command to clone the repo and run `install.sh`:

```bash
git clone https://github.com/Divitare/animal-recording.git && cd animal-recording && chmod +x install.sh && ./install.sh
```

Recommended update command from a fresh checkout:

```bash
cd /root && rm -rf animal-recording-update && git clone https://github.com/Divitare/animal-recording.git animal-recording-update && cd animal-recording-update && chmod +x install.sh && ./install.sh update
```

The installer will:

- elevate with `sudo` if needed
- install missing system packages
- create a service user
- copy the project into `/opt/bird-monitor`
- create a Python virtual environment
- install Python dependencies
- verify that BirdNET can actually be imported and used
- initialize the database
- start the server with `systemd` when available, or with `nohup` otherwise

If it finds an existing installation, it will offer to update it or completely uninstall it.
Updates are downloaded from `https://github.com/Divitare/animal-recording.git`, so re-running `install.sh` pulls the latest server code instead of only reusing the current local copy.
The installer now writes a detailed log file under `/tmp/bird-monitor-logs` and prints a summary with warnings and failing commands at the end of each install or update run.
It also records the deployed Git commit in `/opt/bird-monitor/installed-commit.txt` for quick verification after an update.

## Notes

- Bird activity detection is heuristic and may still produce false positives from insects, wind, or machinery.
- The dashboard starts on the last six hours and the visible time span is controlled in the browser.
- Exported zip files include a `manifest.csv` with timestamps and bird-event counts.
