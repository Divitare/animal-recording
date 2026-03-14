const dashboardState = {
  recordings: [],
  status: null,
  livePollHandle: null,
};

const dashboardElements = {
  rangeForm: document.querySelector("#range-form"),
  rangeStart: document.querySelector("#range-start"),
  rangeEnd: document.querySelector("#range-end"),
  refreshButton: document.querySelector("#refresh-button"),
  downloadButton: document.querySelector("#download-button"),
  manualStartButton: document.querySelector("#manual-start-button"),
  manualStopButton: document.querySelector("#manual-stop-button"),
  timelineSummary: document.querySelector("#timeline-summary"),
  timelineEmpty: document.querySelector("#timeline-empty"),
  timelineDays: document.querySelector("#timeline-days"),
  serviceState: document.querySelector("#service-state"),
  serviceSummary: document.querySelector("#service-summary"),
  activityMessage: document.querySelector("#activity-message"),
  activityDetail: document.querySelector("#activity-detail"),
  activityMode: document.querySelector("#activity-mode"),
  liveLevel: document.querySelector("#live-level"),
  waveformCanvas: document.querySelector("#waveform-canvas"),
  serviceError: document.querySelector("#service-error"),
  totalRecordings: document.querySelector("#total-recordings"),
  totalDetections: document.querySelector("#total-detections"),
  currentDevice: document.querySelector("#current-device"),
  speciesState: document.querySelector("#species-state"),
};

function dashboardFetchJson(url, options = {}) {
  return fetch(url, options).then(async (response) => {
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
  });
}

function dashboardShowError(error) {
  dashboardElements.serviceError.textContent = error.message || String(error);
}

function dashboardDatetimeLocalValue(date) {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  const hours = `${date.getHours()}`.padStart(2, "0");
  const minutes = `${date.getMinutes()}`.padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function dashboardSetDefaultRange() {
  const end = new Date();
  const start = new Date(end.getTime() - (72 * 60 * 60 * 1000));
  dashboardElements.rangeStart.value = dashboardDatetimeLocalValue(start);
  dashboardElements.rangeEnd.value = dashboardDatetimeLocalValue(end);
}

function dashboardBindEvents() {
  dashboardElements.rangeForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await dashboardLoadRecordings();
    } catch (error) {
      dashboardShowError(error);
    }
  });

  dashboardElements.refreshButton.addEventListener("click", async () => {
    try {
      await dashboardRefreshAll();
    } catch (error) {
      dashboardShowError(error);
    }
  });

  dashboardElements.downloadButton.addEventListener("click", async () => {
    try {
      const params = new URLSearchParams({
        start: new Date(dashboardElements.rangeStart.value).toISOString(),
        end: new Date(dashboardElements.rangeEnd.value).toISOString(),
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
      dashboardShowError(error);
    }
  });

  dashboardElements.manualStartButton.addEventListener("click", async () => {
    try {
      const payload = await dashboardFetchJson("/api/manual-recording/start", { method: "POST" });
      dashboardRenderService(payload.service);
    } catch (error) {
      dashboardShowError(error);
    }
  });

  dashboardElements.manualStopButton.addEventListener("click", async () => {
    try {
      const payload = await dashboardFetchJson("/api/manual-recording/stop", { method: "POST" });
      dashboardRenderService(payload.service);
    } catch (error) {
      dashboardShowError(error);
    }
  });
}

async function dashboardRefreshAll() {
  await dashboardLoadStatus();
  await dashboardLoadRecordings();
}

async function dashboardLoadStatus() {
  const payload = await dashboardFetchJson("/api/status");
  dashboardState.status = payload.service;
  dashboardRenderStatus(payload);
}

async function dashboardLoadLiveStatus() {
  const payload = await dashboardFetchJson("/api/live");
  dashboardState.status = payload.service;
  dashboardRenderService(payload.service);
}

function dashboardStartLivePolling() {
  if (dashboardState.livePollHandle !== null) {
    clearInterval(dashboardState.livePollHandle);
  }
  dashboardState.livePollHandle = window.setInterval(async () => {
    try {
      await dashboardLoadLiveStatus();
    } catch (error) {
      dashboardShowError(error);
    }
  }, 1000);
}

async function dashboardLoadRecordings() {
  const params = new URLSearchParams({
    start: new Date(dashboardElements.rangeStart.value).toISOString(),
    end: new Date(dashboardElements.rangeEnd.value).toISOString(),
  });
  const payload = await dashboardFetchJson(`/api/recordings?${params.toString()}`);
  dashboardState.recordings = payload.items;
  dashboardRenderTimeline(payload.range);
}

function dashboardRenderStatus(payload) {
  dashboardRenderService(payload.service || {});
  dashboardElements.totalRecordings.textContent = `${payload.totals.recordings}`;
  dashboardElements.totalDetections.textContent = `${payload.totals.detections}`;
}

function dashboardRenderService(service) {
  const isRecording = Boolean(service.is_recording);
  const manualMode = Boolean(service.manual_mode);
  const reason = service.activity_reason || "idle";
  const activeSchedules = service.active_schedule_names || [];

  if (reason === "manual" || reason === "manual-armed") {
    dashboardElements.serviceState.textContent = isRecording ? "Manual recording" : "Manual ready";
  } else if (reason === "schedule") {
    dashboardElements.serviceState.textContent = isRecording ? "Scheduled recording" : "Watching schedule";
  } else {
    dashboardElements.serviceState.textContent = Boolean(service.started) ? "Idle" : "Recorder disabled";
  }

  dashboardElements.serviceState.classList.toggle("is-recording", isRecording);
  dashboardElements.serviceState.classList.toggle("is-manual", reason === "manual" || reason === "manual-armed");
  dashboardElements.serviceSummary.textContent = service.activity_message || "Waiting for recorder state...";
  dashboardElements.activityMessage.textContent = service.activity_message || "Waiting for recorder state...";
  dashboardElements.activityDetail.textContent = dashboardBuildActivityDetail(service, activeSchedules);
  dashboardElements.activityMode.textContent = dashboardBuildModeLabel(reason, manualMode, isRecording);
  dashboardElements.currentDevice.textContent = service.current_device_name || "Auto selection";
  dashboardElements.speciesState.textContent = dashboardBuildSpeciesState(service);
  dashboardElements.serviceError.textContent = service.last_error || "";
  dashboardElements.liveLevel.textContent = `Input ${Math.round((service.live_level || 0) * 100)}%`;
  dashboardElements.manualStartButton.disabled = manualMode;
  dashboardElements.manualStopButton.disabled = !manualMode;

  dashboardRenderWaveform(service.waveform_samples || []);
}

function dashboardBuildSpeciesState(service) {
  if (service.species_enabled) {
    return "BirdNET active";
  }
  if (service.species_provider === "birdnet") {
    return service.species_available === false ? "BirdNET unavailable" : "BirdNET selected";
  }
  return "Activity markers only";
}

function dashboardBuildActivityDetail(service, activeSchedules) {
  if (service.activity_reason === "manual" || service.activity_reason === "manual-armed" || service.manual_mode) {
    return "Manual control overrides the schedule until you press Stop.";
  }
  if (activeSchedules.length) {
    return `Active schedules: ${activeSchedules.join(", ")}`;
  }
  if (service.last_recording_at) {
    return `Last recording ended at ${new Date(service.last_recording_at).toLocaleString()}`;
  }
  return "No active schedules right now.";
}

function dashboardBuildModeLabel(reason, manualMode, isRecording) {
  if (reason === "manual" || reason === "manual-armed" || manualMode) {
    return isRecording ? "Manual mode live" : "Manual mode armed";
  }
  if (reason === "schedule") {
    return isRecording ? "Scheduled recording live" : "Schedule monitoring";
  }
  return "Idle";
}

function dashboardRenderWaveform(samples) {
  const canvas = dashboardElements.waveformCanvas;
  const context = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;

  context.clearRect(0, 0, width, height);
  context.fillStyle = "#fff9eb";
  context.fillRect(0, 0, width, height);

  context.strokeStyle = "rgba(47, 36, 23, 0.15)";
  context.lineWidth = 1;
  context.beginPath();
  context.moveTo(0, height / 2);
  context.lineTo(width, height / 2);
  context.stroke();

  if (!samples.length) {
    return;
  }

  context.strokeStyle = "#1f6b45";
  context.lineWidth = 2;
  context.beginPath();
  samples.forEach((value, index) => {
    const x = (index / Math.max(samples.length - 1, 1)) * width;
    const normalized = Math.max(0, Math.min(1, value));
    const y = height / 2 - (normalized * height * 0.38);
    if (index === 0) {
      context.moveTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.stroke();

  context.strokeStyle = "rgba(240, 122, 40, 0.65)";
  context.beginPath();
  samples.forEach((value, index) => {
    const x = (index / Math.max(samples.length - 1, 1)) * width;
    const normalized = Math.max(0, Math.min(1, value));
    const y = height / 2 + (normalized * height * 0.38);
    if (index === 0) {
      context.moveTo(x, y);
    } else {
      context.lineTo(x, y);
    }
  });
  context.stroke();
}

function dashboardRenderTimeline(range) {
  dashboardElements.timelineDays.innerHTML = "";

  if (!dashboardState.recordings.length) {
    dashboardElements.timelineEmpty.style.display = "block";
    dashboardElements.timelineSummary.textContent = `No recordings found between ${new Date(range.start).toLocaleString()} and ${new Date(range.end).toLocaleString()}.`;
    return;
  }

  dashboardElements.timelineEmpty.style.display = "none";
  const totalDetections = dashboardState.recordings.reduce(
    (sum, recording) => sum + (recording.detections?.length || 0),
    0,
  );
  dashboardElements.timelineSummary.textContent = `${dashboardState.recordings.length} recording segment(s) and ${totalDetections} bird detection(s) between ${new Date(range.start).toLocaleString()} and ${new Date(range.end).toLocaleString()}.`;

  const grouped = new Map();
  dashboardState.recordings.forEach((recording) => {
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
      lane.append(dashboardBuildHourStrip());

      items.forEach((recording) => {
        lane.append(dashboardBuildRecordingBlock(recording));
      });

      row.append(lane);
      row.append(dashboardBuildDayDetectionList(items));
      dashboardElements.timelineDays.append(row);
    });
}

function dashboardBuildHourStrip() {
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

function dashboardBuildRecordingBlock(recording) {
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
  block.title = dashboardBuildRecordingTitle(recording, localStart, localEnd);

  const label = document.createElement("span");
  label.className = "recording-block-label";
  label.textContent = `${localStart.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
  block.append(label);

  recording.detections.forEach((detection) => {
    const detectionStart = new Date(detection.started_at);
    const offsetPercent = (((detectionStart - localStart) / 1000) / durationSeconds) * 100;
    const marker = document.createElement("span");
    marker.className = `bird-marker${detection.species_common_name ? " is-species" : ""}`;
    marker.style.left = `${Math.min(Math.max(offsetPercent, 0), 100)}%`;
    marker.title = dashboardBuildDetectionTitle(detection);
    block.append(marker);
  });

  return block;
}

function dashboardBuildRecordingTitle(recording, localStart, localEnd) {
  const detections = recording.detections || [];
  const summary = detections.length
    ? detections.map((detection) => `${new Date(detection.started_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} ${dashboardDetectionLabel(detection)}`).join(", ")
    : "No bird detections";
  return `${localStart.toLocaleTimeString()} - ${localEnd.toLocaleTimeString()} | ${summary}`;
}

function dashboardBuildDayDetectionList(recordings) {
  const container = document.createElement("div");
  container.className = "day-detection-list";

  const items = recordings
    .flatMap((recording) => (recording.detections || []).map((detection) => ({
      detection,
      audioUrl: recording.audio_url,
    })))
    .sort((left, right) => new Date(left.detection.started_at) - new Date(right.detection.started_at));

  if (!items.length) {
    container.innerHTML = `<div class="day-detection-empty">No bird detections were found in these recordings.</div>`;
    return container;
  }

  items.forEach(({ detection, audioUrl }) => {
    const link = document.createElement("a");
    link.className = `detection-chip${detection.species_common_name ? " has-species" : ""}`;
    link.href = audioUrl;
    link.target = "_blank";
    link.rel = "noopener";
    link.title = dashboardBuildDetectionTitle(detection);
    link.textContent = `${new Date(detection.started_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} ${dashboardDetectionLabel(detection)}`;
    container.append(link);
  });

  return container;
}

function dashboardDetectionLabel(detection) {
  if (detection.species_common_name) {
    return detection.species_common_name;
  }
  return detection.source === "birdnet" ? "Bird detection" : "Bird activity";
}

function dashboardBuildDetectionTitle(detection) {
  const start = new Date(detection.started_at).toLocaleTimeString();
  const end = new Date(detection.ended_at).toLocaleTimeString();
  const species = detection.species_common_name || "Unclassified bird activity";
  const scientific = detection.species_scientific_name ? ` (${detection.species_scientific_name})` : "";
  const score = detection.species_score != null ? ` | species ${detection.species_score.toFixed(2)}` : "";
  return `${species}${scientific} | ${start} - ${end} | confidence ${detection.confidence.toFixed(2)}${score}`;
}

async function initDashboard() {
  dashboardSetDefaultRange();
  dashboardBindEvents();
  dashboardRenderWaveform(new Array(120).fill(0));
  try {
    await dashboardRefreshAll();
    dashboardStartLivePolling();
  } catch (error) {
    dashboardShowError(error);
  }
}

document.addEventListener("DOMContentLoaded", initDashboard);
