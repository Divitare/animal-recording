const BASE_PIXELS_PER_HOUR = 220;
const MIN_ZOOM = 0.45;
const MAX_ZOOM = 4;

const dashboardState = {
  recordings: [],
  speciesEvents: [],
  speciesStats: [],
  range: null,
  mergeGapSeconds: 600,
  status: null,
  livePollHandle: null,
  zoomFactor: 1,
  timelineDragPointerId: null,
  timelineDragStartX: 0,
  timelineDragStartScrollLeft: 0,
  timelineDidDrag: false,
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
  timelineScroll: document.querySelector("#timeline-scroll"),
  timelineCanvas: document.querySelector("#timeline-canvas"),
  zoomLabel: document.querySelector("#zoom-label"),
  statsSummary: document.querySelector("#stats-summary"),
  statsGrid: document.querySelector("#stats-grid"),
  speciesStatsList: document.querySelector("#species-stats-list"),
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
  const start = new Date(end.getTime() - (6 * 60 * 60 * 1000));
  dashboardElements.rangeStart.value = dashboardDatetimeLocalValue(start);
  dashboardElements.rangeEnd.value = dashboardDatetimeLocalValue(end);
}

function dashboardBindEvents() {
  dashboardElements.rangeForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await dashboardLoadRecordings(true);
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

  dashboardElements.timelineScroll.addEventListener("wheel", (event) => {
    if (!dashboardState.range) {
      return;
    }
    if (Math.abs(event.deltaX) > Math.abs(event.deltaY) && !event.ctrlKey) {
      return;
    }
    event.preventDefault();
    const zoomStep = event.deltaY < 0 ? 1.15 : (1 / 1.15);
    dashboardZoomTimeline(zoomStep, event.clientX);
  }, { passive: false });

  dashboardElements.timelineScroll.addEventListener("pointerdown", (event) => {
    if (event.button !== 0) {
      return;
    }
    dashboardState.timelineDragPointerId = event.pointerId;
    dashboardState.timelineDragStartX = event.clientX;
    dashboardState.timelineDragStartScrollLeft = dashboardElements.timelineScroll.scrollLeft;
    dashboardState.timelineDidDrag = false;
    dashboardElements.timelineScroll.classList.add("is-dragging");
    dashboardElements.timelineScroll.setPointerCapture(event.pointerId);
  });

  dashboardElements.timelineScroll.addEventListener("pointermove", (event) => {
    if (dashboardState.timelineDragPointerId !== event.pointerId) {
      return;
    }
    const deltaX = event.clientX - dashboardState.timelineDragStartX;
    if (Math.abs(deltaX) > 3) {
      dashboardState.timelineDidDrag = true;
    }
    dashboardElements.timelineScroll.scrollLeft = dashboardState.timelineDragStartScrollLeft - deltaX;
  });

  const releaseTimelineDrag = (event) => {
    if (dashboardState.timelineDragPointerId !== event.pointerId) {
      return;
    }
    dashboardElements.timelineScroll.classList.remove("is-dragging");
    if (dashboardElements.timelineScroll.hasPointerCapture(event.pointerId)) {
      dashboardElements.timelineScroll.releasePointerCapture(event.pointerId);
    }
    dashboardState.timelineDragPointerId = null;
    window.setTimeout(() => {
      dashboardState.timelineDidDrag = false;
    }, 0);
  };

  dashboardElements.timelineScroll.addEventListener("pointerup", releaseTimelineDrag);
  dashboardElements.timelineScroll.addEventListener("pointercancel", releaseTimelineDrag);
  dashboardElements.timelineScroll.addEventListener("click", (event) => {
    if (!dashboardState.timelineDidDrag) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
  }, true);
}

async function dashboardRefreshAll() {
  await dashboardLoadStatus();
  await dashboardLoadRecordings(false);
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

async function dashboardLoadRecordings(resetZoom) {
  const previousScrollRatio = dashboardCurrentScrollRatio();
  const params = new URLSearchParams({
    start: new Date(dashboardElements.rangeStart.value).toISOString(),
    end: new Date(dashboardElements.rangeEnd.value).toISOString(),
  });
  const payload = await dashboardFetchJson(`/api/recordings?${params.toString()}`);
  dashboardState.recordings = payload.items || [];
  dashboardState.speciesEvents = payload.species_events || [];
  dashboardState.speciesStats = payload.species_stats || [];
  dashboardState.mergeGapSeconds = payload.species_event_merge_gap_seconds || 600;
  dashboardState.range = payload.range;
  if (resetZoom) {
    dashboardFitTimeline();
  }
  dashboardRenderTimeline();
  if (!resetZoom) {
    dashboardRestoreScrollRatio(previousScrollRatio);
  }
  dashboardRenderStatistics();
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
  dashboardElements.serviceError.textContent = dashboardBuildServiceProblems(service);
  dashboardElements.liveLevel.textContent = `Input ${Math.round((service.live_level || 0) * 100)}%`;
  dashboardElements.manualStartButton.disabled = manualMode;
  dashboardElements.manualStopButton.disabled = !manualMode;

  dashboardRenderWaveform(service.waveform_samples || []);
}

function dashboardBuildSpeciesState(service) {
  if (service.species_enabled) {
    return "BirdNET species detection active";
  }
  if (service.species_provider === "birdnet") {
    return service.species_available === false ? "BirdNET unavailable" : "BirdNET selected";
  }
  return "Activity markers only";
}

function dashboardBuildServiceProblems(service) {
  const messages = [];
  if (service.last_error) {
    messages.push(`Recorder: ${service.last_error}`);
  }
  if (service.species_error) {
    messages.push(`BirdNET: ${service.species_error}`);
  }
  return messages.join(" ");
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

function dashboardFitTimeline() {
  if (!dashboardState.range) {
    dashboardState.zoomFactor = 1;
    return;
  }

  const durationHours = dashboardRangeDurationHours();
  const availableWidth = Math.max(dashboardElements.timelineScroll.clientWidth - 40, 400);
  const computedZoom = availableWidth / Math.max(durationHours * BASE_PIXELS_PER_HOUR, 1);
  dashboardState.zoomFactor = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, computedZoom));
}

function dashboardCurrentScrollRatio() {
  const scrollElement = dashboardElements.timelineScroll;
  const maxScroll = Math.max(scrollElement.scrollWidth - scrollElement.clientWidth, 0);
  if (maxScroll <= 0) {
    return 0;
  }
  return scrollElement.scrollLeft / maxScroll;
}

function dashboardZoomTimeline(multiplier, clientX) {
  if (!dashboardState.range) {
    return;
  }

  const scrollElement = dashboardElements.timelineScroll;
  const previousWidth = dashboardTimelineWidth();
  const nextZoom = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, dashboardState.zoomFactor * multiplier));
  if (Math.abs(nextZoom - dashboardState.zoomFactor) < 0.001) {
    return;
  }

  const rect = scrollElement.getBoundingClientRect();
  const anchorWithinView = clientX != null ? clientX - rect.left : (scrollElement.clientWidth / 2);
  const anchorRatio = (scrollElement.scrollLeft + anchorWithinView) / Math.max(previousWidth, 1);

  dashboardState.zoomFactor = nextZoom;
  dashboardRenderTimeline();

  const nextWidth = dashboardTimelineWidth();
  const nextScrollLeft = (anchorRatio * nextWidth) - anchorWithinView;
  scrollElement.scrollLeft = Math.max(0, Math.min(nextScrollLeft, Math.max(nextWidth - scrollElement.clientWidth, 0)));
}

function dashboardRestoreScrollRatio(ratio) {
  const scrollElement = dashboardElements.timelineScroll;
  const maxScroll = Math.max(scrollElement.scrollWidth - scrollElement.clientWidth, 0);
  scrollElement.scrollLeft = Math.max(0, Math.min(maxScroll * ratio, maxScroll));
}

function dashboardRenderTimeline() {
  dashboardElements.timelineCanvas.innerHTML = "";

  if (!dashboardState.range) {
    dashboardElements.timelineEmpty.style.display = "block";
    dashboardElements.timelineSummary.textContent = "No data loaded yet.";
    dashboardUpdateZoomLabel();
    return;
  }

  const totalDetections = dashboardState.recordings.reduce(
    (sum, recording) => sum + (recording.detections?.length || 0),
    0,
  );
  dashboardElements.timelineSummary.textContent = `${dashboardState.recordings.length} recording segment(s), ${dashboardState.speciesEvents.length} merged species event(s), and ${totalDetections} raw detection(s) between ${dashboardFormatDateTime(dashboardState.range.start)} and ${dashboardFormatDateTime(dashboardState.range.end)}.`;
  dashboardUpdateZoomLabel();

  if (!dashboardState.recordings.length && !dashboardState.speciesEvents.length) {
    dashboardElements.timelineEmpty.style.display = "block";
    dashboardElements.timelineEmpty.textContent = "No recordings or species detections were found in this time span.";
    return;
  }

  dashboardElements.timelineEmpty.style.display = "none";

  const canvas = document.createElement("div");
  canvas.className = "timeline-range";
  const width = dashboardTimelineWidth();
  canvas.style.width = `${width}px`;

  canvas.append(dashboardBuildAxis(width));
  canvas.append(dashboardBuildRangeTrack(width));
  dashboardElements.timelineCanvas.append(canvas);
}

function dashboardBuildAxis(width) {
  const axis = document.createElement("div");
  axis.className = "timeline-axis";
  const ticks = dashboardBuildTicks(width);
  ticks.forEach((tick) => axis.append(tick));
  return axis;
}

function dashboardBuildRangeTrack(width) {
  const track = document.createElement("div");
  track.className = "timeline-range-track";

  const recordingLane = document.createElement("div");
  recordingLane.className = "recording-lane";
  dashboardState.recordings.forEach((recording) => {
    recordingLane.append(dashboardBuildRecordingBlock(recording, width));
  });
  track.append(recordingLane);

  const speciesLane = document.createElement("div");
  speciesLane.className = "species-lane";
  dashboardState.speciesEvents.forEach((event, index) => {
    speciesLane.append(dashboardBuildSpeciesEventChip(event, index, width));
  });
  track.append(speciesLane);

  return track;
}

function dashboardBuildTicks(width) {
  const ticks = [];
  const start = new Date(dashboardState.range.start);
  const end = new Date(dashboardState.range.end);
  const durationHours = dashboardRangeDurationHours();
  const intervalMinutes = dashboardSelectTickMinutes(durationHours, width);
  const firstTick = new Date(start);
  firstTick.setSeconds(0, 0);
  firstTick.setMinutes(Math.ceil(firstTick.getMinutes() / intervalMinutes) * intervalMinutes);

  if (firstTick < start) {
    firstTick.setMinutes(firstTick.getMinutes() + intervalMinutes);
  }

  for (let tick = new Date(firstTick); tick <= end; tick = new Date(tick.getTime() + (intervalMinutes * 60 * 1000))) {
    const marker = document.createElement("div");
    marker.className = "time-tick";
    marker.style.left = `${dashboardRangeRatio(tick) * width}px`;
    marker.innerHTML = `
      <span class="time-tick-line"></span>
      <span class="time-tick-label">${tick.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</span>
    `;
    ticks.push(marker);
  }

  return ticks;
}

function dashboardSelectTickMinutes(durationHours, width) {
  const maxTicks = Math.max(Math.floor(width / 140), 3);
  const candidates = [5, 10, 15, 30, 60, 120, 180, 360, 720];
  return candidates.find((candidate) => ((durationHours * 60) / candidate) <= maxTicks) || 720;
}

function dashboardBuildRecordingBlock(recording, width) {
  const block = document.createElement("a");
  block.className = `recording-segment${recording.has_bird_activity ? " has-birds" : ""}`;
  block.href = recording.audio_url;
  block.target = "_blank";
  block.rel = "noopener";

  const start = new Date(recording.started_at);
  const end = new Date(recording.ended_at);
  const left = dashboardRangeRatio(start) * width;
  const right = dashboardRangeRatio(end) * width;
  const blockWidth = Math.max(right - left, 6);

  block.style.left = `${left}px`;
  block.style.width = `${blockWidth}px`;
  block.title = dashboardBuildRecordingTitle(recording, start, end);

  const label = document.createElement("span");
  label.className = "recording-segment-label";
  label.textContent = `${start.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
  block.append(label);
  return block;
}

function dashboardBuildRecordingTitle(recording, localStart, localEnd) {
  const detections = recording.detections || [];
  const summary = detections.length
    ? detections.map((detection) => dashboardBuildDetectionInline(detection)).join(", ")
    : "No detections";
  return `${localStart.toLocaleTimeString()} - ${localEnd.toLocaleTimeString()} | ${summary}`;
}

function dashboardBuildSpeciesEventChip(event, index, width) {
  const chip = document.createElement("div");
  chip.className = "species-event-chip";
  chip.style.top = `${10 + ((index % 3) * 38)}px`;

  const start = new Date(event.started_at);
  const end = new Date(event.ended_at);
  const left = dashboardRangeRatio(start) * width;
  const right = dashboardRangeRatio(end) * width;
  const naturalWidth = Math.max(right - left, 140);
  const chipWidth = Math.min(Math.max(naturalWidth, 140), 260);
  const maxLeft = Math.max(width - chipWidth - 6, 0);

  chip.style.left = `${Math.min(left, maxLeft)}px`;
  chip.style.width = `${chipWidth}px`;
  chip.title = dashboardBuildSpeciesEventTitle(event);
  chip.innerHTML = `
    <strong>${event.species_common_name}</strong>
    <span>${Math.round((event.confidence || 0) * 100)}%</span>
  `;
  return chip;
}

function dashboardBuildSpeciesEventTitle(event) {
  const scientific = event.species_scientific_name ? ` (${event.species_scientific_name})` : "";
  return `${event.species_common_name}${scientific} | ${dashboardFormatDateTime(event.started_at)} - ${dashboardFormatDateTime(event.ended_at)} | best ${Math.round((event.confidence || 0) * 100)}% | average ${Math.round((event.average_confidence || 0) * 100)}% | ${event.detection_count} merged detection(s)`;
}

function dashboardRenderStatistics() {
  dashboardElements.statsGrid.innerHTML = "";
  dashboardElements.speciesStatsList.innerHTML = "";

  if (!dashboardState.range) {
    dashboardElements.statsSummary.textContent = "No species events loaded yet.";
    return;
  }

  const mergedEvents = dashboardState.speciesEvents.length;
  const speciesCount = dashboardState.speciesStats.length;
  const bestConfidence = dashboardState.speciesEvents.reduce(
    (best, event) => Math.max(best, Number(event.confidence || 0)),
    0,
  );
  const lastSeen = dashboardState.speciesEvents.length
    ? dashboardState.speciesEvents.reduce((latest, event) => new Date(event.ended_at) > new Date(latest.ended_at) ? event : latest).ended_at
    : null;

  dashboardElements.statsSummary.textContent = `${mergedEvents} merged species event(s) across ${speciesCount} species in the selected range. Same-species detections within ${Math.round(dashboardState.mergeGapSeconds / 60)} minutes count as one event.`;

  const cards = [
    { label: "Species", value: `${speciesCount}` },
    { label: "Merged events", value: `${mergedEvents}` },
    { label: "Best confidence", value: `${Math.round(bestConfidence * 100)}%` },
    { label: "Last recognized", value: lastSeen ? new Date(lastSeen).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "-" },
  ];

  cards.forEach((card) => {
    const wrapper = document.createElement("article");
    wrapper.className = "stat-card";
    wrapper.innerHTML = `
      <span>${card.label}</span>
      <strong>${card.value}</strong>
    `;
    dashboardElements.statsGrid.append(wrapper);
  });

  if (!dashboardState.speciesStats.length) {
    dashboardElements.speciesStatsList.innerHTML = `<div class="empty-state">No species were identified in this time span.</div>`;
    return;
  }

  dashboardState.speciesStats.forEach((item) => {
    const row = document.createElement("article");
    row.className = "species-stat-row";
    const scientific = item.species_scientific_name ? `<span class="species-scientific">${item.species_scientific_name}</span>` : "";
    row.innerHTML = `
      <div>
        <strong>${item.species_common_name}</strong>
        ${scientific}
      </div>
      <div class="species-stat-metrics">
        <span>${item.event_count} event(s)</span>
        <span>${Math.round((item.average_confidence || 0) * 100)}% avg</span>
        <span>${Math.round((item.best_confidence || 0) * 100)}% best</span>
        <span>${item.last_seen_at ? `last ${new Date(item.last_seen_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}` : "-"}</span>
      </div>
    `;
    dashboardElements.speciesStatsList.append(row);
  });
}

function dashboardBuildDetectionInline(detection) {
  const label = detection.species_common_name || "Bird activity";
  const confidence = detection.species_score != null ? detection.species_score : detection.confidence;
  return `${new Date(detection.started_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} ${label} ${Math.round((confidence || 0) * 100)}%`;
}

function dashboardUpdateZoomLabel() {
  dashboardElements.zoomLabel.textContent = `Zoom ${Math.round(dashboardState.zoomFactor * 100)}%`;
}

function dashboardRangeDurationHours() {
  if (!dashboardState.range) {
    return 1;
  }
  const start = new Date(dashboardState.range.start);
  const end = new Date(dashboardState.range.end);
  return Math.max((end - start) / (1000 * 60 * 60), 1 / 60);
}

function dashboardTimelineWidth() {
  const width = dashboardRangeDurationHours() * BASE_PIXELS_PER_HOUR * dashboardState.zoomFactor;
  return Math.max(width, dashboardElements.timelineScroll.clientWidth - 24, 720);
}

function dashboardRangeRatio(value) {
  const target = value instanceof Date ? value : new Date(value);
  const start = new Date(dashboardState.range.start);
  const end = new Date(dashboardState.range.end);
  const ratio = (target - start) / Math.max(end - start, 1);
  return Math.max(0, Math.min(1, ratio));
}

function dashboardFormatDateTime(value) {
  return new Date(value).toLocaleString();
}

async function initDashboard() {
  dashboardSetDefaultRange();
  dashboardBindEvents();
  dashboardRenderWaveform(new Array(120).fill(0));
  try {
    await dashboardLoadStatus();
    await dashboardLoadRecordings(true);
    dashboardStartLivePolling();
  } catch (error) {
    dashboardShowError(error);
  }
}

document.addEventListener("DOMContentLoaded", initDashboard);
