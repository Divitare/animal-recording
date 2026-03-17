# V1 Legacy App

This folder contains the current legacy single-node Bird Monitor application.

Use the repository root installer to install or update it:

```bash
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install.sh | sudo bash
```

For local development inside `v1/`:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
python -m bird_monitor
```

The long-term replacement work happens in `../v2/`.
