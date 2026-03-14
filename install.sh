#!/usr/bin/env bash
set -Eeuo pipefail

APP_NAME="bird-monitor"
SERVICE_NAME="bird-monitor"
SERVICE_USER="birdmonitor"
REPO_URL="https://github.com/Divitare/animal-recording.git"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/bird-monitor}"
CURRENT_DIR="${INSTALL_ROOT}/current"
VENV_DIR="${INSTALL_ROOT}/.venv"
DATA_DIR="${DATA_DIR:-/var/lib/bird-monitor}"
LOG_DIR="${LOG_DIR:-/var/log/bird-monitor}"
ENV_FILE="${ENV_FILE:-/etc/bird-monitor.env}"
SYSTEMD_UNIT="/etc/systemd/system/${SERVICE_NAME}.service"
PID_FILE="${DATA_DIR}/${SERVICE_NAME}.pid"
RUN_MODE_FILE="${INSTALL_ROOT}/.run-mode"
COMMIT_FILE="${INSTALL_ROOT}/installed-commit.txt"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION="${1:-auto}"
PACKAGE_MANAGER=""
PYTHON_BIN=""
SOURCE_DIR=""
SOURCE_IS_TEMP="false"
INSTALL_LOG_DIR="${TMPDIR:-/tmp}/${APP_NAME}-logs"
INSTALL_LOG_FILE=""
CURRENT_STAGE="startup"
SUMMARY_PRINTED="false"
declare -a WARNING_LOG=()
declare -a ERROR_LOG=()

log() {
  printf '[%s] %s\n' "${APP_NAME}" "$*"
}

warn() {
  WARNING_LOG+=("$*")
  printf '[%s] warning: %s\n' "${APP_NAME}" "$*" >&2
}

die() {
  ERROR_LOG+=("$*")
  printf '[%s] error: %s\n' "${APP_NAME}" "$*" >&2
  exit 1
}

prepare_run_logging() {
  if [[ -n "${INSTALL_LOG_FILE}" ]]; then
    return
  fi

  mkdir -p "${INSTALL_LOG_DIR}"
  INSTALL_LOG_FILE="${INSTALL_LOG_DIR}/${APP_NAME}-$(date +%Y%m%dT%H%M%S).log"
  touch "${INSTALL_LOG_FILE}"
  chmod 600 "${INSTALL_LOG_FILE}" || true
  exec > >(tee -a "${INSTALL_LOG_FILE}") 2>&1
  log "Detailed installer log: ${INSTALL_LOG_FILE}"
}

set_stage() {
  CURRENT_STAGE="$*"
  log "${CURRENT_STAGE}"
}

print_summary() {
  local exit_code="$1"
  if [[ "${SUMMARY_PRINTED}" == "true" ]]; then
    return
  fi

  SUMMARY_PRINTED="true"

  printf '\n[%s] ----- installation summary -----\n' "${APP_NAME}"
  if [[ "${exit_code}" -eq 0 ]]; then
    printf '[%s] result: success\n' "${APP_NAME}"
  else
    printf '[%s] result: failed\n' "${APP_NAME}"
  fi
  printf '[%s] action: %s\n' "${APP_NAME}" "${ACTION}"
  printf '[%s] stage: %s\n' "${APP_NAME}" "${CURRENT_STAGE}"
  if [[ -n "${INSTALL_LOG_FILE}" ]]; then
    printf '[%s] log file: %s\n' "${APP_NAME}" "${INSTALL_LOG_FILE}"
  fi
  if [[ "${#WARNING_LOG[@]}" -gt 0 ]]; then
    printf '[%s] warnings:\n' "${APP_NAME}"
    for item in "${WARNING_LOG[@]}"; do
      printf '  - %s\n' "${item}"
    done
  fi
  if [[ "${#ERROR_LOG[@]}" -gt 0 ]]; then
    printf '[%s] errors:\n' "${APP_NAME}"
    for item in "${ERROR_LOG[@]}"; do
      printf '  - %s\n' "${item}"
    done
  fi
}

cleanup_on_error() {
  local line_number="$1"
  local failed_command="$2"
  local exit_code="$3"
  cleanup_source_checkout
  ERROR_LOG+=("Command failed at line ${line_number}: ${failed_command}")
  ERROR_LOG+=("Exit code: ${exit_code}")
  print_summary "${exit_code}"
  exit "${exit_code}"
}

finalize_install() {
  local exit_code="$?"
  cleanup_source_checkout
  print_summary "${exit_code}"
}

trap 'cleanup_on_error "${LINENO}" "${BASH_COMMAND}" "$?"' ERR
trap 'finalize_install' EXIT

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

require_root() {
  if [[ "${EUID}" -eq 0 ]]; then
    return
  fi
  if ! command_exists sudo; then
    die "This script needs root privileges. Re-run it as root or install sudo."
  fi
  log "Re-running with sudo to fix permissions and install packages."
  exec sudo -E bash "$0" "$@"
}

detect_package_manager() {
  if command_exists apt-get; then
    PACKAGE_MANAGER="apt"
    return
  fi
  if command_exists dnf; then
    PACKAGE_MANAGER="dnf"
    return
  fi
  if command_exists yum; then
    PACKAGE_MANAGER="yum"
    return
  fi
  if command_exists pacman; then
    PACKAGE_MANAGER="pacman"
    return
  fi
  if command_exists zypper; then
    PACKAGE_MANAGER="zypper"
    return
  fi
  die "Unsupported Linux distribution. No known package manager was found."
}

install_system_packages() {
  log "Installing system packages with ${PACKAGE_MANAGER}."
  case "${PACKAGE_MANAGER}" in
    apt)
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3 python3-venv python3-dev build-essential pkg-config \
        portaudio19-dev libsndfile1 ffmpeg rsync zip unzip curl alsa-utils git
      ;;
    dnf)
      dnf install -y \
        python3 python3-pip python3-devel gcc gcc-c++ make pkgconf-pkg-config \
        portaudio-devel libsndfile ffmpeg rsync zip unzip curl alsa-utils git
      ;;
    yum)
      yum install -y \
        python3 python3-pip python3-devel gcc gcc-c++ make pkgconfig \
        portaudio-devel libsndfile ffmpeg rsync zip unzip curl alsa-utils git
      ;;
    pacman)
      pacman -Sy --noconfirm \
        python python-pip base-devel pkgconf portaudio libsndfile ffmpeg rsync zip unzip curl alsa-utils git
      ;;
    zypper)
      zypper --non-interactive install \
        python3 python3-pip python3-devel gcc gcc-c++ make pkg-config \
        portaudio-devel libsndfile1 ffmpeg rsync zip unzip curl alsa-utils git
      ;;
  esac
}

resolve_python() {
  if command_exists python3; then
    PYTHON_BIN="$(command -v python3)"
    return
  fi
  if command_exists python; then
    PYTHON_BIN="$(command -v python)"
    return
  fi
  die "Python was not installed successfully."
}

ensure_service_user() {
  if id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    return
  fi
  log "Creating service user ${SERVICE_USER}."
  useradd --system --home "${DATA_DIR}" --shell /usr/sbin/nologin "${SERVICE_USER}" 2>/dev/null \
    || useradd --system --home "${DATA_DIR}" --shell /bin/false "${SERVICE_USER}"
}

ensure_directories() {
  mkdir -p "${INSTALL_ROOT}" "${CURRENT_DIR}" "${DATA_DIR}" "${LOG_DIR}"
  chown -R "${SERVICE_USER}:${SERVICE_USER}" "${CURRENT_DIR}" "${DATA_DIR}" "${LOG_DIR}"
  chmod 755 "${INSTALL_ROOT}" "${CURRENT_DIR}" "${DATA_DIR}" "${LOG_DIR}"
}

repair_runtime_permissions() {
  chown -R "${SERVICE_USER}:${SERVICE_USER}" "${CURRENT_DIR}" "${DATA_DIR}" "${LOG_DIR}"
  find "${DATA_DIR}" -type d -exec chmod 755 {} +
  find "${LOG_DIR}" -type d -exec chmod 755 {} +
  find "${DATA_DIR}" -type f -exec chmod 664 {} +
  find "${LOG_DIR}" -type f -exec chmod 664 {} +
}

generate_secret() {
  "${PYTHON_BIN}" - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
}

ensure_env_file() {
  set_stage "Ensuring environment configuration"
  mkdir -p "$(dirname "${ENV_FILE}")"
  if [[ ! -f "${ENV_FILE}" ]]; then
    log "Creating ${ENV_FILE}."
    cat > "${ENV_FILE}" <<EOF
BIRD_MONITOR_SECRET_KEY=$(generate_secret)
BIRD_MONITOR_HOST=0.0.0.0
BIRD_MONITOR_PORT=8080
BIRD_MONITOR_DATA_DIR=${DATA_DIR}
BIRD_MONITOR_DEVICE_NAME=
BIRD_MONITOR_DEVICE_INDEX=
BIRD_MONITOR_SAMPLE_RATE=32000
BIRD_MONITOR_CHANNELS=1
BIRD_MONITOR_SEGMENT_SECONDS=60
BIRD_MONITOR_MIN_EVENT_DURATION_SECONDS=0.2
BIRD_MONITOR_DISABLE_RECORDER=false
BIRD_MONITOR_LOCATION_NAME=
BIRD_MONITOR_LATITUDE=
BIRD_MONITOR_LONGITUDE=
BIRD_MONITOR_SPECIES_PROVIDER=birdnet
BIRD_MONITOR_SPECIES_MIN_CONFIDENCE=0.35
EOF
  fi

  grep -q '^BIRD_MONITOR_DATA_DIR=' "${ENV_FILE}" || echo "BIRD_MONITOR_DATA_DIR=${DATA_DIR}" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_HOST=' "${ENV_FILE}" || echo "BIRD_MONITOR_HOST=0.0.0.0" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_PORT=' "${ENV_FILE}" || echo "BIRD_MONITOR_PORT=8080" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_SAMPLE_RATE=' "${ENV_FILE}" || echo "BIRD_MONITOR_SAMPLE_RATE=32000" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_CHANNELS=' "${ENV_FILE}" || echo "BIRD_MONITOR_CHANNELS=1" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_SEGMENT_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_SEGMENT_SECONDS=60" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_MIN_EVENT_DURATION_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_MIN_EVENT_DURATION_SECONDS=0.2" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_DISABLE_RECORDER=' "${ENV_FILE}" || echo "BIRD_MONITOR_DISABLE_RECORDER=false" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_LOCATION_NAME=' "${ENV_FILE}" || echo "BIRD_MONITOR_LOCATION_NAME=" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_LATITUDE=' "${ENV_FILE}" || echo "BIRD_MONITOR_LATITUDE=" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_LONGITUDE=' "${ENV_FILE}" || echo "BIRD_MONITOR_LONGITUDE=" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_SPECIES_PROVIDER=' "${ENV_FILE}" || echo "BIRD_MONITOR_SPECIES_PROVIDER=birdnet" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_SPECIES_MIN_CONFIDENCE=' "${ENV_FILE}" || echo "BIRD_MONITOR_SPECIES_MIN_CONFIDENCE=0.35" >> "${ENV_FILE}"

  chown "root:${SERVICE_USER}" "${ENV_FILE}"
  chmod 640 "${ENV_FILE}"
}

prepare_source_checkout() {
  set_stage "Downloading latest source code"
  SOURCE_DIR="$(mktemp -d)"
  SOURCE_IS_TEMP="true"
  if ! git clone --depth 1 "${REPO_URL}" "${SOURCE_DIR}"; then
    cleanup_source_checkout
    die "Could not download the latest code from ${REPO_URL}. Check git and network access."
  fi
}

cleanup_source_checkout() {
  if [[ "${SOURCE_IS_TEMP}" == "true" ]] && [[ -n "${SOURCE_DIR}" ]] && [[ -d "${SOURCE_DIR}" ]]; then
    rm -rf "${SOURCE_DIR}"
  fi
  SOURCE_DIR=""
  SOURCE_IS_TEMP="false"
}

sync_source() {
  set_stage "Copying application files into ${CURRENT_DIR}"
  if [[ -z "${SOURCE_DIR}" ]] || [[ ! -d "${SOURCE_DIR}" ]]; then
    die "No downloaded source tree is available."
  fi

  mkdir -p "${CURRENT_DIR}"

  rsync -a --delete \
    --exclude '.git' \
    --exclude 'data' \
    --exclude '.venv' \
    --exclude '__pycache__' \
    --exclude '.pytest_cache' \
    "${SOURCE_DIR}/" "${CURRENT_DIR}/"

  chmod +x "${CURRENT_DIR}/install.sh" "${CURRENT_DIR}/run_server.sh"
  if [[ -d "${SOURCE_DIR}/.git" ]]; then
    mkdir -p "${CURRENT_DIR}/.git"
    rsync -a --delete "${SOURCE_DIR}/.git/" "${CURRENT_DIR}/.git/"
    git -C "${SOURCE_DIR}" rev-parse --short HEAD > "${COMMIT_FILE}" 2>/dev/null || true
    chmod 644 "${COMMIT_FILE}" 2>/dev/null || true
  fi
  chown -R "${SERVICE_USER}:${SERVICE_USER}" "${CURRENT_DIR}"
}

create_virtualenv() {
  set_stage "Creating Python virtual environment"
  rm -rf "${VENV_DIR}"
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
  "${VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel
  "${VENV_DIR}/bin/pip" install -r "${CURRENT_DIR}/requirements.txt"
  install_species_runtime
}

install_species_runtime() {
  set_stage "Installing optional BirdNET runtime dependencies"
  if "${VENV_DIR}/bin/python" -c "import birdnetlib, librosa" >/dev/null 2>&1; then
    log "BirdNET Python packages are available."
  elif "${VENV_DIR}/bin/pip" install birdnetlib librosa; then
    log "Installed BirdNET Python packages."
  else
    warn "BirdNET Python packages could not be installed automatically. Recording will still work, but species labels will remain unavailable until birdnetlib and librosa are installed in ${VENV_DIR}."
    return
  fi

  if "${VENV_DIR}/bin/python" -c "import tflite_runtime" >/dev/null 2>&1; then
    log "TensorFlow Lite runtime is already available for BirdNET."
    return
  fi

  log "Installing TensorFlow Lite runtime for BirdNET."
  if "${VENV_DIR}/bin/pip" install tflite-runtime; then
    return
  fi

  warn "tflite-runtime could not be installed automatically. Trying full TensorFlow as a fallback."
  if "${VENV_DIR}/bin/pip" install tensorflow; then
    return
  fi

  warn "BirdNET runtime packages could not be installed automatically. Recording will still work, but species labels will remain unavailable until TensorFlow Lite or TensorFlow is installed in ${VENV_DIR}."
}

initialize_database() {
  set_stage "Initializing application database"
  su -s /bin/bash -c "cd '${CURRENT_DIR}' && set -a && source '${ENV_FILE}' && set +a && BIRD_MONITOR_DISABLE_RECORDER=true '${VENV_DIR}/bin/python' -c \"from bird_monitor.app import create_app; create_app()\"" "${SERVICE_USER}"
  repair_runtime_permissions
}

has_systemd() {
  [[ -d /run/systemd/system ]] && command_exists systemctl
}

write_systemd_unit() {
  set_stage "Installing systemd service unit"
  sed \
    -e "s|__SERVICE_USER__|${SERVICE_USER}|g" \
    -e "s|__INSTALL_ROOT__|${INSTALL_ROOT}|g" \
    -e "s|__ENV_FILE__|${ENV_FILE}|g" \
    "${CURRENT_DIR}/deploy/bird-monitor.service" > "${SYSTEMD_UNIT}"
  chmod 644 "${SYSTEMD_UNIT}"
}

stop_process_if_running() {
  if has_systemd && systemctl list-unit-files | grep -q "^${SERVICE_NAME}\.service"; then
    systemctl stop "${SERVICE_NAME}" || true
  fi

  if [[ -f "${PID_FILE}" ]]; then
    local pid
    pid="$(cat "${PID_FILE}")"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" || true
    fi
    rm -f "${PID_FILE}"
  fi
}

start_service() {
  if has_systemd; then
    set_stage "Starting systemd service"
    write_systemd_unit
    echo "systemd" > "${RUN_MODE_FILE}"
    systemctl daemon-reload
    systemctl enable --now "${SERVICE_NAME}"
    return
  fi

  set_stage "Starting server with nohup fallback"
  echo "nohup" > "${RUN_MODE_FILE}"
  touch "${LOG_DIR}/server.log"
  chown "${SERVICE_USER}:${SERVICE_USER}" "${LOG_DIR}/server.log"
  su -s /bin/bash -c "cd '${CURRENT_DIR}' && set -a && source '${ENV_FILE}' && set +a && nohup ./run_server.sh >> '${LOG_DIR}/server.log' 2>&1 & echo \$! > '${PID_FILE}'" "${SERVICE_USER}"
}

confirm_uninstall() {
  printf 'This will permanently remove the server, recordings, exports, logs, config, and service user.\n'
  read -r -p "Type DELETE to continue: " confirmation
  [[ "${confirmation}" == "DELETE" ]] || die "Uninstall cancelled."
}

remove_service_user() {
  if id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    userdel "${SERVICE_USER}" >/dev/null 2>&1 || true
  fi
}

uninstall_everything() {
  set_stage "Removing Bird Monitor installation"
  confirm_uninstall
  stop_process_if_running

  if has_systemd && [[ -f "${SYSTEMD_UNIT}" ]]; then
    systemctl disable "${SERVICE_NAME}" || true
    rm -f "${SYSTEMD_UNIT}"
    systemctl daemon-reload
  fi

  rm -rf "${INSTALL_ROOT}" "${DATA_DIR}" "${LOG_DIR}"
  rm -f "${ENV_FILE}"
  remove_service_user
  log "Bird Monitor has been completely removed."
}

existing_installation() {
  [[ -d "${INSTALL_ROOT}" || -f "${ENV_FILE}" || -f "${SYSTEMD_UNIT}" ]]
}

choose_action_if_needed() {
  if [[ "${ACTION}" != "auto" ]]; then
    return
  fi

  if ! existing_installation; then
    ACTION="install"
    return
  fi

  printf 'An existing Bird Monitor installation was detected.\n'
  printf '1) Update existing installation\n'
  printf '2) Completely uninstall\n'
  printf '3) Exit\n'
  read -r -p "Choose [1-3]: " choice
  case "${choice}" in
    1) ACTION="update" ;;
    2) ACTION="uninstall" ;;
    3) ACTION="exit" ;;
    *) die "Unknown option." ;;
  esac
}

show_post_install_notes() {
  set_stage "Finalizing installation"
  local port
  port="$(grep '^BIRD_MONITOR_PORT=' "${ENV_FILE}" | tail -n1 | cut -d= -f2-)"
  log "Server is up. Open http://$(hostname -f 2>/dev/null || hostname):${port:-8080}"
  if [[ -f "${COMMIT_FILE}" ]]; then
    log "Installed commit: $(cat "${COMMIT_FILE}")"
  fi
  log "Open /settings in the web interface to configure the microphone, BirdNET species analysis, and recording schedules."
  log "Re-running install.sh will now download the latest code directly from ${REPO_URL} for updates."
  if [[ -f "${RUN_MODE_FILE}" ]] && [[ "$(cat "${RUN_MODE_FILE}")" == "nohup" ]]; then
    log "Logs are being written to ${LOG_DIR}/server.log"
  else
    log "Use 'systemctl status ${SERVICE_NAME}' to inspect the service."
  fi
}

perform_install_or_update() {
  set_stage "Detecting package manager"
  detect_package_manager
  set_stage "Installing required system packages"
  install_system_packages
  set_stage "Resolving Python runtime"
  resolve_python
  set_stage "Ensuring service user exists"
  ensure_service_user
  set_stage "Ensuring runtime directories exist"
  ensure_directories
  ensure_env_file
  set_stage "Stopping existing service"
  stop_process_if_running
  prepare_source_checkout
  sync_source
  cleanup_source_checkout
  create_virtualenv
  set_stage "Repairing runtime permissions"
  repair_runtime_permissions
  initialize_database
  start_service
  show_post_install_notes
}

main() {
  require_root "$@"
  prepare_run_logging
  set_stage "Determining requested action"
  choose_action_if_needed

  case "${ACTION}" in
    auto|install|update)
      perform_install_or_update
      ;;
    uninstall)
      uninstall_everything
      ;;
    exit)
      log "Nothing changed."
      ;;
    *)
      die "Usage: $0 [install|update|uninstall]"
      ;;
  esac
}

main "$@"
