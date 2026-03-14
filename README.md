# Bird Monitor

Bird Monitor is a Flask-based USB microphone recorder for bird sound monitoring. It stores audio files, marks likely bird activity in a timeline, lets you define recurring recording schedules, and can export a selected time span as a zip archive.

## What it does

- Records from a USB microphone during predefined schedule windows
- Lets you manually start and stop recording from the browser
- Shows the live microphone waveform and current recorder activity
- Stores each recording segment as a WAV file and tracks metadata in SQLite
- Detects likely bird activity inside each segment and marks those moments in the timeline
- Shows past recordings in a continuous day-by-day web timeline
- Lets you download every recording that overlaps a selected time span
- Lets you configure the microphone and segment length from the browser

## Species detection

Exact species detection is possible, but not perfectly reliable. In practice it usually needs a dedicated bird-classification model such as BirdNET plus a good microphone, clean audio, and regional context. This project ships with bird activity detection by default and includes an optional hook for BirdNET-style species labeling if you install and enable an external classifier.

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

The installer will:

- elevate with `sudo` if needed
- install missing system packages
- create a service user
- copy the project into `/opt/bird-monitor`
- create a Python virtual environment
- install Python dependencies
- initialize the database
- start the server with `systemd` when available, or with `nohup` otherwise

If it finds an existing installation, it will offer to update it or completely uninstall it.

## Notes

- Bird activity detection is heuristic and may still produce false positives from insects, wind, or machinery.
- The default timeline groups recordings by the local browser day.
- Exported zip files include a `manifest.csv` with timestamps and bird-event counts.
