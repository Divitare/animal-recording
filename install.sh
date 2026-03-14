#!/usr/bin/env bash
set -Eeuo pipefail

APP_NAME="bird-monitor"
SERVICE_NAME="bird-monitor"
SERVICE_USER="birdmonitor"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/bird-monitor}"
CURRENT_DIR="${INSTALL_ROOT}/current"
VENV_DIR="${INSTALL_ROOT}/.venv"
DATA_DIR="${DATA_DIR:-/var/lib/bird-monitor}"
LOG_DIR="${LOG_DIR:-/var/log/bird-monitor}"
ENV_FILE="${ENV_FILE:-/etc/bird-monitor.env}"
SYSTEMD_UNIT="/etc/systemd/system/${SERVICE_NAME}.service"
PID_FILE="${DATA_DIR}/${SERVICE_NAME}.pid"
RUN_MODE_FILE="${INSTALL_ROOT}/.run-mode"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION="${1:-auto}"
PACKAGE_MANAGER=""
PYTHON_BIN=""

log() {
  printf '[%s] %s\n' "${APP_NAME}" "$*"
}

warn() {
  printf '[%s] warning: %s\n' "${APP_NAME}" "$*" >&2
}

die() {
  printf '[%s] error: %s\n' "${APP_NAME}" "$*" >&2
  exit 1
}

cleanup_on_error() {
  local line_number="$1"
  warn "Installation failed near line ${line_number}."
}

trap 'cleanup_on_error "${LINENO}"' ERR

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
        portaudio19-dev libsndfile1 ffmpeg rsync zip unzip curl alsa-utils
      ;;
    dnf)
      dnf install -y \
        python3 python3-pip python3-devel gcc gcc-c++ make pkgconf-pkg-config \
        portaudio-devel libsndfile ffmpeg rsync zip unzip curl alsa-utils
      ;;
    yum)
      yum install -y \
        python3 python3-pip python3-devel gcc gcc-c++ make pkgconfig \
        portaudio-devel libsndfile ffmpeg rsync zip unzip curl alsa-utils
      ;;
    pacman)
      pacman -Sy --noconfirm \
        python python-pip base-devel pkgconf portaudio libsndfile ffmpeg rsync zip unzip curl alsa-utils
      ;;
    zypper)
      zypper --non-interactive install \
        python3 python3-pip python3-devel gcc gcc-c++ make pkg-config \
        portaudio-devel libsndfile1 ffmpeg rsync zip unzip curl alsa-utils
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

generate_secret() {
  "${PYTHON_BIN}" - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
}

ensure_env_file() {
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
BIRD_MONITOR_SPECIES_PROVIDER=disabled
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
  grep -q '^BIRD_MONITOR_SPECIES_PROVIDER=' "${ENV_FILE}" || echo "BIRD_MONITOR_SPECIES_PROVIDER=disabled" >> "${ENV_FILE}"

  chown "root:${SERVICE_USER}" "${ENV_FILE}"
  chmod 640 "${ENV_FILE}"
}

sync_source() {
  log "Copying application files into ${CURRENT_DIR}."
  local source_real target_real
  source_real="$(cd "${SCRIPT_DIR}" && pwd -P)"
  mkdir -p "${CURRENT_DIR}"
  target_real="$(cd "${CURRENT_DIR}" && pwd -P)"

  if [[ "${source_real}" == "${target_real}" ]]; then
    log "Installer is already running from ${CURRENT_DIR}; reusing the current source tree."
    chown -R "${SERVICE_USER}:${SERVICE_USER}" "${CURRENT_DIR}"
    return
  fi

  rsync -a --delete \
    --exclude '.git' \
    --exclude 'data' \
    --exclude '.venv' \
    --exclude '__pycache__' \
    --exclude '.pytest_cache' \
    "${SCRIPT_DIR}/" "${CURRENT_DIR}/"

  chmod +x "${CURRENT_DIR}/install.sh" "${CURRENT_DIR}/run_server.sh"
  chown -R "${SERVICE_USER}:${SERVICE_USER}" "${CURRENT_DIR}"
}

create_virtualenv() {
  log "Creating Python virtual environment."
  rm -rf "${VENV_DIR}"
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
  "${VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel
  "${VENV_DIR}/bin/pip" install -r "${CURRENT_DIR}/requirements.txt"
}

initialize_database() {
  log "Initializing database."
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
  (
    cd "${CURRENT_DIR}"
    BIRD_MONITOR_DISABLE_RECORDER=true "${VENV_DIR}/bin/python" -c "from bird_monitor.app import create_app; create_app()"
  )
}

has_systemd() {
  [[ -d /run/systemd/system ]] && command_exists systemctl
}

write_systemd_unit() {
  log "Installing systemd service unit."
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
    write_systemd_unit
    echo "systemd" > "${RUN_MODE_FILE}"
    systemctl daemon-reload
    systemctl enable --now "${SERVICE_NAME}"
    return
  fi

  log "systemd not found. Starting the server with nohup instead."
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
  local port
  port="$(grep '^BIRD_MONITOR_PORT=' "${ENV_FILE}" | tail -n1 | cut -d= -f2-)"
  log "Server is up. Open http://$(hostname -f 2>/dev/null || hostname):${port:-8080}"
  log "If the USB microphone name changes, edit ${ENV_FILE} or use the web settings page."
  if [[ -f "${RUN_MODE_FILE}" ]] && [[ "$(cat "${RUN_MODE_FILE}")" == "nohup" ]]; then
    log "Logs are being written to ${LOG_DIR}/server.log"
  else
    log "Use 'systemctl status ${SERVICE_NAME}' to inspect the service."
  fi
}

perform_install_or_update() {
  detect_package_manager
  install_system_packages
  resolve_python
  ensure_service_user
  ensure_directories
  ensure_env_file
  stop_process_if_running
  sync_source
  create_virtualenv
  initialize_database
  start_service
  show_post_install_notes
}

main() {
  require_root "$@"
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
