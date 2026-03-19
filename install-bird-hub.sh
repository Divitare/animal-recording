#!/usr/bin/env bash
set -Eeuo pipefail

export BIRD_INSTALL_VARIANT="v2-bird-hub"
curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install.sh | bash -s -- "$@"
