# Animal Recording

This repository now has two tracks:

- `v1`: the current legacy single-node Bird Monitor app as it exists today
- `v2`: the new multi-role codebase that will grow into `bird-node` and `bird-hub`

From now on, active development should happen inside `v2/`.

## Install Or Update

Use this same command for both first-time installation and later updates:

```bash
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install.sh | sudo bash
```

If you want to skip the first-install menu and choose the variant directly:

```bash
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install.sh | sudo BIRD_INSTALL_VARIANT=v2-bird-node bash
```

What it does:

- on the first installation it asks which variant to install:
  - `v1` legacy single-node app
  - `v2 bird-node`
  - `v2 bird-hub`
- on later runs it automatically detects the installed variant and updates that same variant

## Repository Layout

```text
v1/
  bird_monitor/
  deploy/
  tests/
  .env.example
  requirements.txt
  requirements-dev.txt
  run_server.sh

v2/
  bird-node/
  bird-hub/
```

## Notes

- `v1` remains installable and usable as the legacy baseline.
- `v2/bird-node` and `v2/bird-hub` are the new development targets.
- The root `install.sh` is now the only installer/update entry point.
