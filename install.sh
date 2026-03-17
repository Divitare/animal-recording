#!/usr/bin/env bash
set -Eeuo pipefail

INSTALLER_NAME="bird-install"
REPO_URL="https://github.com/Divitare/animal-recording.git"
VARIANT_STATE_FILE="${VARIANT_STATE_FILE:-/etc/bird-install-variant}"
ACTION="${1:-auto}"
PACKAGE_MANAGER=""
PYTHON_BIN=""
SOURCE_DIR=""
SOURCE_IS_TEMP="false"
VENV_REBUILT="false"
INSTALL_LOG_DIR="${TMPDIR:-/tmp}/${INSTALLER_NAME}-logs"
INSTALL_LOG_FILE=""
CURRENT_STAGE="startup"
SUMMARY_PRINTED="false"

INSTALL_VARIANT=""
APP_NAME=""
SERVICE_NAME=""
SERVICE_USER=""
INSTALL_ROOT=""
CURRENT_DIR=""
VENV_DIR=""
DATA_DIR=""
LOG_DIR=""
ENV_FILE=""
SYSTEMD_UNIT=""
PID_FILE=""
RUN_MODE_FILE=""
COMMIT_FILE=""
RELEASE_COMMIT_FILE=""
REQUIREMENTS_HASH_FILE=""
SOURCE_SUBDIR=""
DEFAULT_PORT=""
VERIFY_SPECIES_RUNTIME="false"
INITIALIZE_APP_DATABASE="false"
SPECIES_VERIFY_MODULE=""
STATUS_FILE=""

declare -a WARNING_LOG=()
declare -a ERROR_LOG=()

log() {
  printf '[%s] %s\n' "${INSTALLER_NAME}" "$*"
}

warn() {
  WARNING_LOG+=("$*")
  printf '[%s] warning: %s\n' "${INSTALLER_NAME}" "$*" >&2
}

die() {
  ERROR_LOG+=("$*")
  printf '[%s] error: %s\n' "${INSTALLER_NAME}" "$*" >&2
  exit 1
}

prepare_run_logging() {
  if [[ -n "${INSTALL_LOG_FILE}" ]]; then
    return
  fi

  mkdir -p "${INSTALL_LOG_DIR}"
  INSTALL_LOG_FILE="${INSTALL_LOG_DIR}/${INSTALLER_NAME}-$(date +%Y%m%dT%H%M%S).log"
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

  printf '\n[%s] ----- installation summary -----\n' "${INSTALLER_NAME}"
  if [[ "${exit_code}" -eq 0 ]]; then
    printf '[%s] result: success\n' "${INSTALLER_NAME}"
  else
    printf '[%s] result: failed\n' "${INSTALLER_NAME}"
  fi
  printf '[%s] action: %s\n' "${INSTALLER_NAME}" "${ACTION}"
  printf '[%s] variant: %s\n' "${INSTALLER_NAME}" "${INSTALL_VARIANT:-unknown}"
  printf '[%s] stage: %s\n' "${INSTALLER_NAME}" "${CURRENT_STAGE}"
  if [[ -n "${INSTALL_LOG_FILE}" ]]; then
    printf '[%s] log file: %s\n' "${INSTALLER_NAME}" "${INSTALL_LOG_FILE}"
  fi
  if [[ "${#WARNING_LOG[@]}" -gt 0 ]]; then
    printf '[%s] warnings:\n' "${INSTALLER_NAME}"
    for item in "${WARNING_LOG[@]}"; do
      printf '  - %s\n' "${item}"
    done
  fi
  if [[ "${#ERROR_LOG[@]}" -gt 0 ]]; then
    printf '[%s] errors:\n' "${INSTALLER_NAME}"
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

python_minor_version() {
  "$1" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
}

hash_file() {
  local path="$1"
  if command_exists sha256sum; then
    sha256sum "${path}" | awk '{print $1}'
    return
  fi
  if command_exists shasum; then
    shasum -a 256 "${path}" | awk '{print $1}'
    return
  fi
  "${PYTHON_BIN}" - "${path}" <<'PY'
import hashlib
import pathlib
import sys

print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
PY
}

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[&|]/\\&/g'
}

set_env_value() {
  local key="$1"
  local value="$2"
  local file="$3"
  local escaped_value
  escaped_value="$(escape_sed_replacement "${value}")"

  if grep -q "^${key}=" "${file}" 2>/dev/null; then
    sed -i "s|^${key}=.*|${key}=${escaped_value}|" "${file}"
  else
    printf '%s=%s\n' "${key}" "${value}" >> "${file}"
  fi
}

read_env_value() {
  local key="$1"
  local file="$2"
  grep "^${key}=" "${file}" | tail -n1 | cut -d= -f2-
}

variant_label() {
  case "$1" in
    v1) echo "v1 legacy single-node app" ;;
    v2-bird-node) echo "v2 bird-node" ;;
    v2-bird-hub) echo "v2 bird-hub" ;;
    *) echo "$1" ;;
  esac
}

configure_variant() {
  INSTALL_VARIANT="$1"
  case "${INSTALL_VARIANT}" in
    v1)
      APP_NAME="bird-monitor"
      SERVICE_NAME="bird-monitor"
      SERVICE_USER="birdmonitor"
      INSTALL_ROOT="${INSTALL_ROOT:-/opt/bird-monitor}"
      DATA_DIR="${DATA_DIR:-/var/lib/bird-monitor}"
      LOG_DIR="${LOG_DIR:-/var/log/bird-monitor}"
      ENV_FILE="${ENV_FILE:-/etc/bird-monitor.env}"
      SOURCE_SUBDIR="v1"
      DEFAULT_PORT="8080"
      VERIFY_SPECIES_RUNTIME="true"
      INITIALIZE_APP_DATABASE="true"
      SPECIES_VERIFY_MODULE="bird_monitor.species"
      ;;
    v2-bird-node)
      APP_NAME="bird-node"
      SERVICE_NAME="bird-node"
      SERVICE_USER="birdnode"
      INSTALL_ROOT="${INSTALL_ROOT:-/opt/bird-node}"
      DATA_DIR="${DATA_DIR:-/var/lib/bird-node}"
      LOG_DIR="${LOG_DIR:-/var/log/bird-node}"
      ENV_FILE="${ENV_FILE:-/etc/bird-node.env}"
      SOURCE_SUBDIR="v2/bird-node"
      DEFAULT_PORT="8081"
      VERIFY_SPECIES_RUNTIME="true"
      INITIALIZE_APP_DATABASE="false"
      SPECIES_VERIFY_MODULE="bird_node.species"
      ;;
    v2-bird-hub)
      APP_NAME="bird-hub"
      SERVICE_NAME="bird-hub"
      SERVICE_USER="birdhub"
      INSTALL_ROOT="${INSTALL_ROOT:-/opt/bird-hub}"
      DATA_DIR="${DATA_DIR:-/var/lib/bird-hub}"
      LOG_DIR="${LOG_DIR:-/var/log/bird-hub}"
      ENV_FILE="${ENV_FILE:-/etc/bird-hub.env}"
      SOURCE_SUBDIR="v2/bird-hub"
      DEFAULT_PORT="8080"
      VERIFY_SPECIES_RUNTIME="false"
      INITIALIZE_APP_DATABASE="false"
      SPECIES_VERIFY_MODULE=""
      ;;
    *)
      die "Unknown install variant: ${INSTALL_VARIANT}"
      ;;
  esac

  CURRENT_DIR="${INSTALL_ROOT}/current"
  VENV_DIR="${INSTALL_ROOT}/.venv"
  SYSTEMD_UNIT="/etc/systemd/system/${SERVICE_NAME}.service"
  PID_FILE="${DATA_DIR}/${SERVICE_NAME}.pid"
  RUN_MODE_FILE="${INSTALL_ROOT}/.run-mode"
  COMMIT_FILE="${INSTALL_ROOT}/installed-commit.txt"
  RELEASE_COMMIT_FILE="${CURRENT_DIR}/.release-commit"
  REQUIREMENTS_HASH_FILE="${INSTALL_ROOT}/.requirements.sha256"
  STATUS_FILE="${DATA_DIR}/status.json"
}

detect_installed_variant() {
  local candidates=()
  local known_variant=""

  if [[ -f "${VARIANT_STATE_FILE}" ]]; then
    known_variant="$(tr -d '[:space:]' < "${VARIANT_STATE_FILE}")"
    case "${known_variant}" in
      v1|v2-bird-node|v2-bird-hub)
        printf '%s\n' "${known_variant}"
        return
        ;;
      *)
        warn "Ignoring unknown installed variant marker: ${known_variant}"
        ;;
    esac
  fi

  if [[ -d /opt/bird-monitor/current || -f /etc/bird-monitor.env || -f /etc/systemd/system/bird-monitor.service ]]; then
    candidates+=("v1")
  fi
  if [[ -d /opt/bird-node/current || -f /etc/bird-node.env || -f /etc/systemd/system/bird-node.service ]]; then
    candidates+=("v2-bird-node")
  fi
  if [[ -d /opt/bird-hub/current || -f /etc/bird-hub.env || -f /etc/systemd/system/bird-hub.service ]]; then
    candidates+=("v2-bird-hub")
  fi

  if [[ "${#candidates[@]}" -gt 1 ]]; then
    die "Multiple installed variants were detected (${candidates[*]}). Keep only one install on this machine or set ${VARIANT_STATE_FILE} to the intended variant."
  fi

  if [[ "${#candidates[@]}" -eq 1 ]]; then
    printf '%s\n' "${candidates[0]}"
  fi
}

write_variant_state() {
  printf '%s\n' "${INSTALL_VARIANT}" > "${VARIANT_STATE_FILE}"
  chmod 644 "${VARIANT_STATE_FILE}" || true
}

remove_variant_state() {
  if [[ -f "${VARIANT_STATE_FILE}" ]]; then
    rm -f "${VARIANT_STATE_FILE}"
  fi
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
  local existing_host=""
  local existing_port=""
  local default_node_id=""
  local default_sample_rate="32000"
  mkdir -p "$(dirname "${ENV_FILE}")"
  default_node_id="$(hostname -s 2>/dev/null || hostname)"
  if [[ "${INSTALL_VARIANT}" == "v2-bird-node" ]]; then
    default_sample_rate="16000"
  fi
  if [[ ! -f "${ENV_FILE}" ]]; then
    log "Creating ${ENV_FILE}."
    cat > "${ENV_FILE}" <<EOF
BIRD_MONITOR_SECRET_KEY=$(generate_secret)
BIRD_MONITOR_APP_VARIANT=${INSTALL_VARIANT}
BIRD_MONITOR_APP_COMMIT=
BIRD_MONITOR_HOST=0.0.0.0
BIRD_MONITOR_PORT=${DEFAULT_PORT}
BIRD_MONITOR_DATA_DIR=${DATA_DIR}
BIRD_MONITOR_LOG_DIR=${LOG_DIR}
BIRD_MONITOR_STATUS_FILE=${STATUS_FILE}
BIRD_MONITOR_NODE_ID=${default_node_id}
BIRD_MONITOR_DEVICE_NAME=
BIRD_MONITOR_DEVICE_INDEX=
BIRD_MONITOR_SAMPLE_RATE=${default_sample_rate}
BIRD_MONITOR_CHANNELS=1
BIRD_MONITOR_SEGMENT_SECONDS=30
BIRD_MONITOR_MIN_EVENT_DURATION_SECONDS=0.2
BIRD_MONITOR_LIVE_WINDOW_SECONDS=9
BIRD_MONITOR_LIVE_STEP_SECONDS=3
BIRD_MONITOR_MINIMUM_LIVE_ANALYSIS_SECONDS=3
BIRD_MONITOR_AUDIO_BUFFER_SECONDS=120
BIRD_MONITOR_DETECTION_CLIP_PADDING_SECONDS=0.4
BIRD_MONITOR_STATUS_WRITE_INTERVAL_SECONDS=2.0
BIRD_MONITOR_DISABLE_RECORDER=false
BIRD_MONITOR_LOCATION_NAME=
BIRD_MONITOR_LATITUDE=
BIRD_MONITOR_LONGITUDE=
BIRD_MONITOR_SPECIES_PROVIDER=birdnet
BIRD_MONITOR_SPECIES_MIN_CONFIDENCE=0.35
EOF
  fi

  existing_host="$(read_env_value 'BIRD_MONITOR_HOST' "${ENV_FILE}" 2>/dev/null || true)"
  existing_port="$(read_env_value 'BIRD_MONITOR_PORT' "${ENV_FILE}" 2>/dev/null || true)"

  set_env_value "BIRD_MONITOR_APP_VARIANT" "${INSTALL_VARIANT}" "${ENV_FILE}"
  set_env_value "BIRD_MONITOR_HOST" "${existing_host:-0.0.0.0}" "${ENV_FILE}"
  set_env_value "BIRD_MONITOR_PORT" "${existing_port:-${DEFAULT_PORT}}" "${ENV_FILE}"
  set_env_value "BIRD_MONITOR_DATA_DIR" "${DATA_DIR}" "${ENV_FILE}"
  set_env_value "BIRD_MONITOR_LOG_DIR" "${LOG_DIR}" "${ENV_FILE}"
  set_env_value "BIRD_MONITOR_STATUS_FILE" "${STATUS_FILE}" "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_NODE_ID=' "${ENV_FILE}" || echo "BIRD_MONITOR_NODE_ID=${default_node_id}" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_DEVICE_NAME=' "${ENV_FILE}" || echo "BIRD_MONITOR_DEVICE_NAME=" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_DEVICE_INDEX=' "${ENV_FILE}" || echo "BIRD_MONITOR_DEVICE_INDEX=" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_SAMPLE_RATE=' "${ENV_FILE}" || echo "BIRD_MONITOR_SAMPLE_RATE=${default_sample_rate}" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_CHANNELS=' "${ENV_FILE}" || echo "BIRD_MONITOR_CHANNELS=1" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_SEGMENT_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_SEGMENT_SECONDS=30" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_MIN_EVENT_DURATION_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_MIN_EVENT_DURATION_SECONDS=0.2" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_LIVE_WINDOW_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_LIVE_WINDOW_SECONDS=9" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_LIVE_STEP_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_LIVE_STEP_SECONDS=3" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_MINIMUM_LIVE_ANALYSIS_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_MINIMUM_LIVE_ANALYSIS_SECONDS=3" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_AUDIO_BUFFER_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_AUDIO_BUFFER_SECONDS=120" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_DETECTION_CLIP_PADDING_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_DETECTION_CLIP_PADDING_SECONDS=0.4" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_STATUS_WRITE_INTERVAL_SECONDS=' "${ENV_FILE}" || echo "BIRD_MONITOR_STATUS_WRITE_INTERVAL_SECONDS=2.0" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_DISABLE_RECORDER=' "${ENV_FILE}" || echo "BIRD_MONITOR_DISABLE_RECORDER=false" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_LOCATION_NAME=' "${ENV_FILE}" || echo "BIRD_MONITOR_LOCATION_NAME=" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_LATITUDE=' "${ENV_FILE}" || echo "BIRD_MONITOR_LATITUDE=" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_LONGITUDE=' "${ENV_FILE}" || echo "BIRD_MONITOR_LONGITUDE=" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_SPECIES_PROVIDER=' "${ENV_FILE}" || echo "BIRD_MONITOR_SPECIES_PROVIDER=birdnet" >> "${ENV_FILE}"
  grep -q '^BIRD_MONITOR_SPECIES_MIN_CONFIDENCE=' "${ENV_FILE}" || echo "BIRD_MONITOR_SPECIES_MIN_CONFIDENCE=0.35" >> "${ENV_FILE}"

  chown "root:${SERVICE_USER}" "${ENV_FILE}"
  chmod 640 "${ENV_FILE}"
}

select_install_variant() {
  printf 'No existing installation was detected.\n'
  printf '1) %s\n' "$(variant_label v1)"
  printf '2) %s\n' "$(variant_label v2-bird-node)"
  printf '3) %s\n' "$(variant_label v2-bird-hub)"
  read -r -p "Choose [1-3]: " choice
  case "${choice}" in
    1) INSTALL_VARIANT="v1" ;;
    2) INSTALL_VARIANT="v2-bird-node" ;;
    3) INSTALL_VARIANT="v2-bird-hub" ;;
    *) die "Unknown option." ;;
  esac
}

prepare_source_checkout() {
  set_stage "Downloading latest source code"
  SOURCE_DIR="$(mktemp -d)"
  SOURCE_IS_TEMP="true"
  if ! git clone --depth 1 --filter=blob:none "${REPO_URL}" "${SOURCE_DIR}"; then
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
  local source_variant_dir=""
  local source_commit=""
  local previous_commit=""

  if [[ -z "${SOURCE_DIR}" ]] || [[ ! -d "${SOURCE_DIR}" ]]; then
    die "No downloaded source tree is available."
  fi

  source_variant_dir="${SOURCE_DIR}/${SOURCE_SUBDIR}"
  if [[ ! -d "${source_variant_dir}" ]]; then
    die "The downloaded source does not contain ${SOURCE_SUBDIR}."
  fi

  mkdir -p "${CURRENT_DIR}"

  rsync -a --delete \
    --exclude '.git' \
    --exclude 'data' \
    --exclude '.venv' \
    --exclude '__pycache__' \
    --exclude '.pytest_cache' \
    "${source_variant_dir}/" "${CURRENT_DIR}/"

  cp "${SOURCE_DIR}/install.sh" "${CURRENT_DIR}/install.sh"
  chmod +x "${CURRENT_DIR}/install.sh"
  if [[ -f "${CURRENT_DIR}/run_server.sh" ]]; then
    chmod +x "${CURRENT_DIR}/run_server.sh"
  fi

  previous_commit="$(cat "${COMMIT_FILE}" 2>/dev/null || true)"
  source_commit="$(git -C "${SOURCE_DIR}" rev-parse --short=12 HEAD 2>/dev/null || true)"
  if [[ -n "${source_commit}" ]]; then
    printf '%s\n' "${source_commit}" > "${COMMIT_FILE}"
    printf '%s\n' "${source_commit}" > "${RELEASE_COMMIT_FILE}"
    set_env_value "BIRD_MONITOR_APP_COMMIT" "${source_commit}" "${ENV_FILE}"
    if [[ -n "${previous_commit}" && "${previous_commit}" == "${source_commit}" ]]; then
      log "Downloaded source commit matches the currently installed commit: ${source_commit}"
    elif [[ -n "${previous_commit}" ]]; then
      log "Updating installed commit from ${previous_commit} to ${source_commit}"
    fi
    log "Downloaded source commit: ${source_commit}"
  fi

  chmod 644 "${COMMIT_FILE}" 2>/dev/null || true
  chmod 644 "${RELEASE_COMMIT_FILE}" 2>/dev/null || true
  chown -R "${SERVICE_USER}:${SERVICE_USER}" "${CURRENT_DIR}"
}

ensure_virtualenv() {
  set_stage "Ensuring Python virtual environment"
  local desired_version=""
  local existing_version=""
  desired_version="$(python_minor_version "${PYTHON_BIN}")"

  if [[ -x "${VENV_DIR}/bin/python" && -x "${VENV_DIR}/bin/pip" ]]; then
    existing_version="$(python_minor_version "${VENV_DIR}/bin/python" 2>/dev/null || true)"
    if [[ -n "${existing_version}" && "${existing_version}" == "${desired_version}" ]]; then
      VENV_REBUILT="false"
      log "Reusing existing virtual environment at ${VENV_DIR} (Python ${existing_version})."
      return
    fi
    log "Rebuilding virtual environment because Python changed from ${existing_version:-unknown} to ${desired_version}."
  else
    log "Creating Python virtual environment at ${VENV_DIR}."
  fi

  rm -rf "${VENV_DIR}"
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
  VENV_REBUILT="true"
}

install_species_runtime() {
  if [[ "${VERIFY_SPECIES_RUNTIME}" != "true" ]]; then
    return
  fi

  set_stage "Installing optional BirdNET runtime dependencies"
  if "${VENV_DIR}/bin/python" -c "import birdnetlib, librosa, resampy" >/dev/null 2>&1; then
    log "BirdNET Python packages are available."
  elif "${VENV_DIR}/bin/pip" install birdnetlib librosa resampy; then
    log "Installed BirdNET Python packages."
  else
    warn "BirdNET Python packages could not be installed automatically."
    return
  fi

  if "${VENV_DIR}/bin/python" -c "import tflite_runtime" >/dev/null 2>&1; then
    log "TensorFlow Lite runtime is already available for BirdNET."
    return
  fi

  log "Installing TensorFlow Lite runtime for BirdNET."
  if "${VENV_DIR}/bin/pip" install tflite-runtime; then
    log "Installed TensorFlow Lite runtime for BirdNET."
    return
  fi

  log "tflite-runtime is not available for this platform or Python version. Trying full TensorFlow as a fallback."
  if "${VENV_DIR}/bin/pip" install tensorflow; then
    log "Installed TensorFlow as the BirdNET runtime fallback."
    return
  fi

  warn "BirdNET runtime packages could not be installed automatically."
}

verify_species_runtime() {
  local verify_output=""

  if [[ "${VERIFY_SPECIES_RUNTIME}" != "true" ]]; then
    return
  fi

  set_stage "Verifying BirdNET species runtime"
  local import_statement=""
  import_statement="from ${SPECIES_VERIFY_MODULE} import build_species_classifier"
  if verify_output="$(
    cd "${CURRENT_DIR}" && "${VENV_DIR}/bin/python" - <<PY
${import_statement}

classifier = build_species_classifier()
print(f"provider={classifier.provider_name}")
print(f"available={classifier.available()}")
reason = getattr(classifier, "failure_reason", None)
if reason:
    print(f"reason={reason}")
details = getattr(classifier, "runtime_details", {}) or {}
if details:
    print(f"runtime_backend={details.get('runtime_backend', 'unknown')}")
    print(f"analysis_mode={details.get('analysis_mode', 'unknown')}")
    for name, version in sorted((details.get('packages') or {}).items()):
        print(f"package_{name}={version or 'missing'}")

raise SystemExit(0 if classifier.available() else 1)
PY
  )"; then
    log "BirdNET verification passed."
    printf '%s\n' "${verify_output}"
    return
  fi

  if [[ -n "${verify_output}" ]]; then
    printf '%s\n' "${verify_output}"
  fi
  die "BirdNET verification did not pass. Installation stopped so the system does not start without working species detection."
}

sync_python_dependencies() {
  set_stage "Syncing Python dependencies"
  local current_requirements_hash=""
  local installed_requirements_hash=""

  current_requirements_hash="$(hash_file "${CURRENT_DIR}/requirements.txt")"
  installed_requirements_hash="$(cat "${REQUIREMENTS_HASH_FILE}" 2>/dev/null || true)"

  if [[ "${VENV_REBUILT}" != "true" ]] && [[ "${current_requirements_hash}" == "${installed_requirements_hash}" ]] && [[ -x "${VENV_DIR}/bin/python" && -x "${VENV_DIR}/bin/pip" ]]; then
    log "requirements.txt is unchanged (${current_requirements_hash}); reusing installed core Python packages."
  else
    log "Installing core Python packages from requirements.txt."
    "${VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel
    "${VENV_DIR}/bin/pip" install -r "${CURRENT_DIR}/requirements.txt"
    printf '%s\n' "${current_requirements_hash}" > "${REQUIREMENTS_HASH_FILE}"
    chmod 644 "${REQUIREMENTS_HASH_FILE}" 2>/dev/null || true
  fi

  install_species_runtime
  verify_species_runtime
}

initialize_application() {
  if [[ "${INITIALIZE_APP_DATABASE}" != "true" ]]; then
    return
  fi

  set_stage "Initializing application database"
  su -s /bin/bash -c "cd '${CURRENT_DIR}' && set -a && source '${ENV_FILE}' && set +a && BIRD_MONITOR_DISABLE_RECORDER=true '${VENV_DIR}/bin/python' -c \"from bird_monitor.app import create_app; create_app()\"" "${SERVICE_USER}"
  repair_runtime_permissions
}

has_systemd() {
  [[ -d /run/systemd/system ]] && command_exists systemctl
}

write_systemd_unit() {
  set_stage "Installing systemd service unit"
  cat > "${SYSTEMD_UNIT}" <<EOF
[Unit]
Description=$(variant_label "${INSTALL_VARIANT}")
After=network.target sound.target
Wants=network.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${CURRENT_DIR}
EnvironmentFile=-${ENV_FILE}
ExecStart=${CURRENT_DIR}/run_server.sh
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF
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

verify_headless_node_service() {
  local expected_commit actual_commit started_flag attempt
  expected_commit="$(cat "${RELEASE_COMMIT_FILE}" 2>/dev/null || true)"

  for attempt in $(seq 1 25); do
    if [[ -f "${STATUS_FILE}" ]]; then
      actual_commit="$(
        STATUS_JSON_PATH="${STATUS_FILE}" "${PYTHON_BIN}" - <<'PY'
import json
import os
from pathlib import Path

payload = json.loads(Path(os.environ["STATUS_JSON_PATH"]).read_text(encoding="utf-8"))
print(((payload.get("app") or {}).get("commit") or "").strip())
PY
      )"
      started_flag="$(
        STATUS_JSON_PATH="${STATUS_FILE}" "${PYTHON_BIN}" - <<'PY'
import json
import os
from pathlib import Path

payload = json.loads(Path(os.environ["STATUS_JSON_PATH"]).read_text(encoding="utf-8"))
print(str(bool((payload.get("service") or {}).get("started"))))
PY
      )"
      if [[ "${started_flag}" == "True" && ( -z "${expected_commit}" || "${actual_commit}" == "${expected_commit}" ) ]]; then
        log "Verified headless node status file: ${STATUS_FILE}"
        return
      fi
    fi
    sleep 2
  done

  die "The bird-node service did not produce a valid status file at ${STATUS_FILE} after installation."
}

verify_running_service() {
  if [[ "${INSTALL_VARIANT}" == "v2-bird-node" ]]; then
    set_stage "Verifying headless bird-node service"
    verify_headless_node_service
    return
  fi

  set_stage "Verifying running service version"
  local port expected_commit response actual_commit attempt
  port="$(read_env_value 'BIRD_MONITOR_PORT' "${ENV_FILE}")"
  port="${port:-${DEFAULT_PORT}}"
  expected_commit="$(cat "${RELEASE_COMMIT_FILE}" 2>/dev/null || true)"

  for attempt in $(seq 1 25); do
    response="$(curl -fsS --max-time 5 "http://127.0.0.1:${port}/api/status" 2>/dev/null || true)"
    if [[ -n "${response}" ]]; then
      actual_commit="$(
        RESPONSE_JSON="${response}" "${PYTHON_BIN}" -c "import json, os; payload=json.loads(os.environ['RESPONSE_JSON']); print(((payload.get('app') or {}).get('commit') or '').strip())"
      )"
      if [[ -z "${expected_commit}" || "${actual_commit}" == "${expected_commit}" ]]; then
        log "Verified running server commit: ${actual_commit:-unknown}"
        return
      fi
    fi
    sleep 2
  done

  if [[ -n "${expected_commit}" ]]; then
    die "The server did not come up on the expected commit ${expected_commit}. Check the installer log, systemctl status ${SERVICE_NAME}, and the service logs."
  fi
  die "The server did not respond successfully on http://127.0.0.1:${port}/api/status after installation."
}

confirm_uninstall() {
  printf 'This will permanently remove the installed variant, its recordings or data, logs, config, and service user.\n'
  read -r -p "Type DELETE to continue: " confirmation
  [[ "${confirmation}" == "DELETE" ]] || die "Uninstall cancelled."
}

remove_service_user() {
  if id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    userdel "${SERVICE_USER}" >/dev/null 2>&1 || true
  fi
}

uninstall_everything() {
  set_stage "Removing installed variant"
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
  remove_variant_state
  log "$(variant_label "${INSTALL_VARIANT}") has been completely removed."
}

choose_action_if_needed() {
  local detected_variant=""

  case "${ACTION}" in
    auto|install|update|uninstall)
      ;;
    *)
      die "Usage: $0 [install|update|uninstall]"
      ;;
  esac

  detected_variant="$(detect_installed_variant || true)"

  if [[ "${ACTION}" == "uninstall" ]]; then
    [[ -n "${detected_variant}" ]] || die "No installed variant was detected to uninstall."
    configure_variant "${detected_variant}"
    return
  fi

  if [[ -n "${detected_variant}" ]]; then
    configure_variant "${detected_variant}"
    if [[ "${ACTION}" == "install" ]]; then
      log "Detected existing $(variant_label "${INSTALL_VARIANT}") installation. Running an update instead."
      ACTION="update"
    elif [[ "${ACTION}" == "auto" ]]; then
      ACTION="update"
    fi
    log "Detected installed variant: $(variant_label "${INSTALL_VARIANT}")"
    return
  fi

  ACTION="install"
  select_install_variant
  configure_variant "${INSTALL_VARIANT}"
  log "Selected install variant: $(variant_label "${INSTALL_VARIANT}")"
}

show_post_install_notes() {
  set_stage "Finalizing installation"
  local port
  port="$(read_env_value 'BIRD_MONITOR_PORT' "${ENV_FILE}")"
  log "Installed variant: $(variant_label "${INSTALL_VARIANT}")"
  if [[ "${INSTALL_VARIANT}" == "v2-bird-node" ]]; then
    log "bird-node is running headless."
    log "Status file: ${STATUS_FILE}"
  else
    log "Server is up. Open http://$(hostname -f 2>/dev/null || hostname):${port:-${DEFAULT_PORT}}"
  fi
  if [[ -f "${COMMIT_FILE}" ]]; then
    log "Installed commit: $(cat "${COMMIT_FILE}")"
  fi
  log "Run this same command again later to update the installed variant automatically:"
  log "curl -fsSL https://raw.githubusercontent.com/Divitare/animal-recording/main/install.sh | sudo bash"
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
  ensure_virtualenv
  sync_python_dependencies
  set_stage "Repairing runtime permissions"
  repair_runtime_permissions
  initialize_application
  start_service
  verify_running_service
  write_variant_state
  show_post_install_notes
}

main() {
  require_root "$@"
  prepare_run_logging
  set_stage "Determining requested action"
  choose_action_if_needed

  case "${ACTION}" in
    install|update)
      perform_install_or_update
      ;;
    uninstall)
      uninstall_everything
      ;;
    *)
      die "Usage: $0 [install|update|uninstall]"
      ;;
  esac
}

main "$@"
