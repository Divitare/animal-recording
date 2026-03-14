const weekdayLabels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

const state = {
  devices: [],
  recordings: [],
  schedules: [],
  status: null,
  settings: null,
  selectedDays: new Set([0, 1, 2, 3, 4, 5, 6]),
};

const elements = {
  rangeForm: document.querySelector("#range-form"),
  rangeStart: document.querySelector("#range-start"),
  rangeEnd: document.querySelector("#range-end"),
  refreshButton: document.querySelector("#refresh-button"),
  downloadButton: document.querySelector("#download-button"),
  settingsForm: document.querySelector("#settings-form"),
  deviceIndex: document.querySelector("#device-index"),
  deviceName: document.querySelector("#device-name"),
  sampleRate: document.querySelector("#sample-rate"),
  channels: document.querySelector("#channels"),
  segmentSeconds: document.querySelector("#segment-seconds"),
  minEventDuration: document.querySelector("#min-event-duration"),
  devicesNote: document.querySelector("#devices-note"),
  scheduleForm: document.querySelector("#schedule-form"),
  scheduleName: document.querySelector("#schedule-name"),
  scheduleStart: document.querySelector("#schedule-start"),
  scheduleEnd: document.querySelector("#schedule-end"),
  scheduleEnabled: document.querySelector("#schedule-enabled"),
  weekdayPicker: document.querySelector("#weekday-picker"),
  scheduleList: document.querySelector("#schedule-list"),
  timelineSummary: document.querySelector("#timeline-summary"),
  timelineEmpty: document.querySelector("#timeline-empty"),
  timelineDays: document.querySelector("#timeline-days"),
  serviceState: document.querySelector("#service-state"),
  serviceSummary: document.querySelector("#service-summary"),
  serviceError: document.querySelector("#service-error"),
  totalRecordings: document.querySelector("#total-recordings"),
  totalDetections: document.querySelector("#total-detections"),
  currentDevice: document.querySelector("#current-device"),
  speciesState: document.querySelector("#species-state"),
};

function toDatetimeLocalValue(date) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  const hours = `${date.getHours()}`.padStart(2, "0");
  const minutes = `${date.getMinutes()}`.padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function setDefaultRange() {
  const end = new Date();
  const start = new Date(end.getTime() - (72 * 60 * 60 * 1000));
  elements.rangeStart.value = toDatetimeLocalValue(start);
  elements.rangeEnd.value = toDatetimeLocalValue(end);
  elements.scheduleStart.value = "05:00";
  elements.scheduleEnd.value = "08:00";
}

function buildWeekdayPicker() {
  weekdayLabels.forEach((label, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "weekday-chip is-selected";
    button.textContent = label;
    button.dataset.day = `${index}`;
    button.addEventListener("click", () => {
      if (state.selectedDays.has(index)) {
        state.selectedDays.delete(index);
      } else {
        state.selectedDays.add(index);
      }
      button.classList.toggle("is-selected", state.selectedDays.has(index));
    });
    elements.weekdayPicker.append(button);
  });
}

function bindEvents() {
  elements.rangeForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await loadRecordings();
    } catch (error) {
      showError(error);
    }
  });

  elements.refreshButton.addEventListener("click", async () => {
    try {
      await refreshAll();
    } catch (error) {
      showError(error);
    }
  });

  elements.downloadButton.addEventListener("click", async () => {
    try {
      const params = new URLSearchParams({
        start: new Date(elements.rangeStart.value).toISOString(),
        end: new Date(elements.rangeEnd.value).toISOString(),
      });
      const response = await fetch(`/api/export?${params.toString()}`);
      if (!response.ok) {
        const payload = await response.json();
        throw new Error(payload.error || "Export failed");
      }
      const blob = await response.blob();
      const disposition = response.headers.get("content-disposition") || "";
      const fileNameMatch = disposition.match(/filename="?([^"]+)"?/i);
      const fileName = fileNameMatch ? fileNameMatch[1] : "bird-recordings.zip";
      const blobUrl = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = blobUrl;
      link.download = fileName;
      document.body.append(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(blobUrl);
    } catch (error) {
      showError(error);
    }
  });

  elements.settingsForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const selectedIndex = elements.deviceIndex.value;
      await fetchJson("/api/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          device_index: selectedIndex === "" ? null : Number(selectedIndex),
          device_name: elements.deviceName.value.trim(),
          sample_rate: Number(elements.sampleRate.value),
          channels: Number(elements.channels.value),
          segment_seconds: Number(elements.segmentSeconds.value),
          min_event_duration_seconds: Number(elements.minEventDuration.value),
        }),
      });
      await refreshAll();
    } catch (error) {
      showError(error);
    }
  });

  elements.scheduleForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await fetchJson("/api/schedules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: elements.scheduleName.value.trim(),
          days_of_week: Array.from(state.selectedDays).sort((a, b) => a - b),
          start_time: elements.scheduleStart.value,
          end_time: elements.scheduleEnd.value,
          enabled: elements.scheduleEnabled.checked,
        }),
      });
      elements.scheduleName.value = "";
      await loadSchedules();
    } catch (error) {
      showError(error);
    }
  });
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const contentType = response.headers.get("content-type") || "";
  let payload = {};
  if (contentType.includes("application/json")) {
    payload = await response.json();
  } else {
    payload = { error: await response.text() };
  }
  if (!response.ok) {
    throw new Error(payload.error || "Request failed");
  }
  return payload;
}

function showError(error) {
  elements.serviceError.textContent = error.message || String(error);
}

async function refreshAll() {
  await loadStatus();
  await loadDevices();
  await loadSettings();
  await loadSchedules();
  await loadRecordings();
}

async function loadStatus() {
  const payload = await fetchJson("/api/status");
  state.status = payload.service;
  renderStatus(payload);
}

async function loadDevices() {
  const payload = await fetch("/api/devices");
  const data = await payload.json();
  state.devices = data.items || [];
  renderDevices(data.error || "");
}

async function loadSettings() {
  state.settings = await fetchJson("/api/settings");
  renderSettings();
}

async function loadSchedules() {
  const payload = await fetchJson("/api/schedules");
  state.schedules = payload.items;
  renderSchedules();
}

async function loadRecordings() {
  const params = new URLSearchParams({
    start: new Date(elements.rangeStart.value).toISOString(),
    end: new Date(elements.rangeEnd.value).toISOString(),
  });
  const payload = await fetchJson(`/api/recordings?${params.toString()}`);
  state.recordings = payload.items;
  renderTimeline(payload.range);
}

function renderStatus(payload) {
  const service = payload.service || {};
  const isRecording = Boolean(service.is_recording);
  const isStarted = Boolean(service.started);
  elements.serviceState.textContent = isRecording ? "Recording" : (isStarted ? "Watching schedule" : "Recorder disabled");
  elements.serviceState.classList.toggle("is-recording", isRecording);
  elements.serviceSummary.textContent = isRecording
    ? `Recording because ${service.active_schedule_names?.join(", ") || "a schedule is active"}.`
    : "Waiting for the next active recording window.";
  elements.totalRecordings.textContent = `${payload.totals.recordings}`;
  elements.totalDetections.textContent = `${payload.totals.detections}`;
  elements.currentDevice.textContent = service.current_device_name || "Auto selection";
  elements.speciesState.textContent = service.species_enabled ? `Enabled (${service.species_provider})` : "Activity markers only";
  elements.serviceError.textContent = service.last_error || "";
}

function renderDevices(errorText) {
  elements.deviceIndex.innerHTML = "";
  const autoOption = document.createElement("option");
  autoOption.value = "";
  autoOption.textContent = "Auto select best input device";
  elements.deviceIndex.append(autoOption);

  state.devices.forEach((device) => {
    const option = document.createElement("option");
    option.value = `${device.index}`;
    option.textContent = `${device.name} (${device.max_input_channels} ch)`;
    elements.deviceIndex.append(option);
  });

  if (errorText) {
    elements.devicesNote.textContent = errorText;
    return;
  }
  elements.devicesNote.textContent = state.devices.length
    ? `${state.devices.length} input device(s) detected.`
    : "No input devices detected yet.";
  if (state.settings) {
    elements.deviceIndex.value = state.settings.device_index == null ? "" : `${state.settings.device_index}`;
  }
}

function renderSettings() {
  if (!state.settings) {
    return;
  }
  elements.deviceIndex.value = state.settings.device_index == null ? "" : `${state.settings.device_index}`;
  elements.deviceName.value = state.settings.device_name || "";
  elements.sampleRate.value = `${state.settings.sample_rate}`;
  elements.channels.value = `${state.settings.channels}`;
  elements.segmentSeconds.value = `${state.settings.segment_seconds}`;
  elements.minEventDuration.value = `${state.settings.min_event_duration_seconds}`;
}

function renderSchedules() {
  elements.scheduleList.innerHTML = "";
  if (!state.schedules.length) {
    elements.scheduleList.innerHTML = `<div class="empty-state">No schedules yet. Add one above.</div>`;
    return;
  }

  state.schedules.forEach((schedule) => {
    const wrapper = document.createElement("article");
    wrapper.className = "schedule-item";

    const daysText = schedule.days_of_week.map((day) => weekdayLabels[day]).join(", ");
    wrapper.innerHTML = `
      <div class="schedule-item-header">
        <strong>${schedule.name}</strong>
        <div class="schedule-actions">
          <button type="button" class="secondary-button" data-action="toggle" data-id="${schedule.id}">
            ${schedule.enabled ? "Disable" : "Enable"}
          </button>
          <button type="button" class="danger-button" data-action="delete" data-id="${schedule.id}">
            Delete
          </button>
        </div>
      </div>
      <div class="schedule-meta">${daysText}</div>
      <div class="schedule-meta">${schedule.start_time} - ${schedule.end_time}</div>
    `;
    elements.scheduleList.append(wrapper);
  });

  elements.scheduleList.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", async () => {
      const scheduleId = Number(button.dataset.id);
      const schedule = state.schedules.find((item) => item.id === scheduleId);
      if (!schedule) {
        return;
      }

      if (button.dataset.action === "delete") {
        try {
          await fetchJson(`/api/schedules/${scheduleId}`, { method: "DELETE" });
          await loadSchedules();
        } catch (error) {
          showError(error);
        }
        return;
      }

      try {
        await fetchJson(`/api/schedules/${scheduleId}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name: schedule.name,
            days_of_week: schedule.days_of_week,
            start_time: schedule.start_time,
            end_time: schedule.end_time,
            enabled: !schedule.enabled,
          }),
        });
        await loadSchedules();
      } catch (error) {
        showError(error);
      }
    });
  });
}

function renderTimeline(range) {
  elements.timelineDays.innerHTML = "";

  if (!state.recordings.length) {
    elements.timelineEmpty.style.display = "block";
    elements.timelineSummary.textContent = `No recordings found between ${new Date(range.start).toLocaleString()} and ${new Date(range.end).toLocaleString()}.`;
    return;
  }

  elements.timelineEmpty.style.display = "none";
  elements.timelineSummary.textContent = `${state.recordings.length} recording segment(s) between ${new Date(range.start).toLocaleString()} and ${new Date(range.end).toLocaleString()}.`;

  const grouped = new Map();
  state.recordings.forEach((recording) => {
    const localStart = new Date(recording.started_at);
    const dayKey = `${localStart.getFullYear()}-${`${localStart.getMonth() + 1}`.padStart(2, "0")}-${`${localStart.getDate()}`.padStart(2, "0")}`;
    if (!grouped.has(dayKey)) {
      grouped.set(dayKey, []);
    }
    grouped.get(dayKey).push(recording);
  });

  Array.from(grouped.entries())
    .sort(([left], [right]) => new Date(left) - new Date(right))
    .forEach(([dayKey, items]) => {
      const row = document.createElement("section");
      row.className = "day-row";

      const date = new Date(`${dayKey}T00:00:00`);
      const label = document.createElement("div");
      label.className = "day-label";
      label.innerHTML = `
        <span>${date.toLocaleDateString(undefined, { weekday: "long", year: "numeric", month: "short", day: "numeric" })}</span>
        <span>${items.length} file(s)</span>
      `;
      row.append(label);

      const lane = document.createElement("div");
      lane.className = "timeline-lane";
      lane.append(buildHourStrip());

      items.forEach((recording) => {
        lane.append(buildRecordingBlock(recording));
      });

      row.append(lane);
      elements.timelineDays.append(row);
    });
}

function buildHourStrip() {
  const strip = document.createElement("div");
  strip.className = "hour-strip";
  for (let hour = 0; hour < 24; hour += 1) {
    const label = document.createElement("div");
    label.className = "hour-label";
    label.textContent = `${`${hour}`.padStart(2, "0")}:00`;
    strip.append(label);
  }
  return strip;
}

function buildRecordingBlock(recording) {
  const block = document.createElement("a");
  block.className = `recording-block${recording.has_bird_activity ? " has-birds" : ""}`;
  block.href = recording.audio_url;
  block.target = "_blank";
  block.rel = "noopener";

  const localStart = new Date(recording.started_at);
  const localEnd = new Date(recording.ended_at);
  const startSeconds = (localStart.getHours() * 3600) + (localStart.getMinutes() * 60) + localStart.getSeconds();
  const endSeconds = (localEnd.getHours() * 3600) + (localEnd.getMinutes() * 60) + localEnd.getSeconds();
  const durationSeconds = Math.max((localEnd - localStart) / 1000, 1);
  const laneEndSeconds = localEnd.toDateString() === localStart.toDateString() ? endSeconds : 86400;
  const widthPercent = Math.max(((Math.max(laneEndSeconds, startSeconds + 1) - startSeconds) / 86400) * 100, 0.4);
  const leftPercent = (startSeconds / 86400) * 100;

  block.style.left = `${leftPercent}%`;
  block.style.width = `${widthPercent}%`;
  block.title = `${localStart.toLocaleTimeString()} - ${localEnd.toLocaleTimeString()} | ${recording.bird_event_count} bird event(s)`;

  const label = document.createElement("span");
  label.className = "recording-block-label";
  label.textContent = `${localStart.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
  block.append(label);

  recording.detections.forEach((detection) => {
    const detectionStart = new Date(detection.started_at);
    const offsetPercent = (((detectionStart - localStart) / 1000) / durationSeconds) * 100;
    const marker = document.createElement("span");
    marker.className = "bird-marker";
    marker.style.left = `${Math.min(Math.max(offsetPercent, 0), 100)}%`;
    const speciesText = detection.species_common_name ? ` | ${detection.species_common_name}` : "";
    marker.title = `Bird activity ${detection.confidence.toFixed(2)} at ${detectionStart.toLocaleTimeString()}${speciesText}`;
    block.append(marker);
  });

  return block;
}

async function init() {
  setDefaultRange();
  buildWeekdayPicker();
  bindEvents();
  try {
    await refreshAll();
  } catch (error) {
    showError(error);
  }
}

document.addEventListener("DOMContentLoaded", init);
