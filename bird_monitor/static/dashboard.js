const BASE_PIXELS_PER_HOUR = 220;
const MIN_ZOOM = 0.45;
const MAX_ZOOM = 4;

const dashboardState = {
  recordings: [],
  detections: [],
  speciesEvents: [],
  speciesStats: [],
  range: null,
  mergeGapSeconds: 600,
  status: null,
  birdnetLogs: [],
  birdnetLogFile: null,
  birdnetLogAutoFollow: true,
  birdnetLogStatusMessage: "Logs are kept live here for BirdNET analysis and recorder activity.",
  birdnetLogStatusTone: "info",
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
  recordingsSummary: document.querySelector("#recordings-summary"),
  recordingsList: document.querySelector("#recordings-list"),
  serviceState: document.querySelector("#service-state"),
  serviceSummary: document.querySelector("#service-summary"),
  activityMessage: document.querySelector("#activity-message"),
  activityDetail: document.querySelector("#activity-detail"),
  activityMode: document.querySelector("#activity-mode"),
  liveLevel: document.querySelector("#live-level"),
  waveformCanvas: document.querySelector("#waveform-canvas"),
  liveDetectionsSummary: document.querySelector("#live-detections-summary"),
  liveDetectionsList: document.querySelector("#live-detections-list"),
  serviceError: document.querySelector("#service-error"),
  totalRecordings: document.querySelector("#total-recordings"),
  totalDetections: document.querySelector("#total-detections"),
  currentDevice: document.querySelector("#current-device"),
  speciesState: document.querySelector("#species-state"),
  pipelineModeNote: document.querySelector("#pipeline-mode-note"),
  pipelineRecordingCard: document.querySelector("#pipeline-recording-card"),
  pipelineRecordingState: document.querySelector("#pipeline-recording-state"),
  pipelineRecordingDetail: document.querySelector("#pipeline-recording-detail"),
  pipelineBirdnetCard: document.querySelector("#pipeline-birdnet-card"),
  pipelineBirdnetState: document.querySelector("#pipeline-birdnet-state"),
  pipelineBirdnetDetail: document.querySelector("#pipeline-birdnet-detail"),
  pipelineClipsCard: document.querySelector("#pipeline-clips-card"),
  pipelineClipsState: document.querySelector("#pipeline-clips-state"),
  pipelineClipsDetail: document.querySelector("#pipeline-clips-detail"),
  pipelineResultCard: document.querySelector("#pipeline-result-card"),
  pipelineResultState: document.querySelector("#pipeline-result-state"),
  pipelineResultDetail: document.querySelector("#pipeline-result-detail"),
  birdnetRuntimeSummary: document.querySelector("#birdnet-runtime-summary"),
  birdnetInstalledState: document.querySelector("#birdnet-installed-state"),
  birdnetBackendState: document.querySelector("#birdnet-backend-state"),
  birdnetLastAnalysisState: document.querySelector("#birdnet-last-analysis-state"),
  birdnetLastTargetState: document.querySelector("#birdnet-last-target-state"),
  birdnetPackageSummary: document.querySelector("#birdnet-package-summary"),
  birdnetLogPath: document.querySelector("#birdnet-log-path"),
  birdnetLogStatus: document.querySelector("#birdnet-log-status"),
  birdnetLogConsole: document.querySelector("#birdnet-log-console"),
  clearLogsButton: document.querySelector("#clear-logs-button"),
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

function dashboardShowLogStatus(message, tone = "info") {
  dashboardState.birdnetLogStatusMessage = message;
  dashboardState.birdnetLogStatusTone = tone;
  dashboardElements.birdnetLogStatus.textContent = message;
  dashboardElements.birdnetLogStatus.dataset.tone = tone;
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

  dashboardElements.clearLogsButton.addEventListener("click", async () => {
    const confirmed = window.confirm("Delete the BirdNET and application log files now?");
    if (!confirmed) {
      return;
    }

    const originalLabel = dashboardElements.clearLogsButton.textContent;
    dashboardElements.clearLogsButton.disabled = true;
    dashboardElements.clearLogsButton.textContent = "Deleting...";
    try {
      const payload = await dashboardFetchJson("/api/birdnet/logs/clear", { method: "POST" });
      dashboardState.birdnetLogs = [];
      dashboardState.birdnetLogAutoFollow = true;
      dashboardRenderBirdnetLogs({ items: [] });
      dashboardShowLogStatus(
        `${payload.message} Cleared ${payload.cleared_count || 0} active file(s) and ${payload.removed_backup_count || 0} rotated backup file(s).`,
        "success",
      );
    } catch (error) {
      dashboardShowError(error);
      dashboardShowLogStatus(`Log deletion failed: ${error.message || String(error)}`, "error");
    } finally {
      dashboardElements.clearLogsButton.disabled = false;
      dashboardElements.clearLogsButton.textContent = originalLabel;
    }
  });

  dashboardElements.birdnetLogConsole.addEventListener("scroll", () => {
    dashboardState.birdnetLogAutoFollow = dashboardLogConsoleIsNearBottom();
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
  await Promise.all([
    dashboardLoadStatus(),
    dashboardLoadRecordings(false),
    dashboardLoadBirdnetLogs(),
  ]);
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

async function dashboardLoadBirdnetLogs() {
  const payload = await dashboardFetchJson("/api/birdnet/logs?limit=80");
  dashboardState.birdnetLogs = payload.items || [];
  dashboardState.birdnetLogFile = payload.log_file || null;
  dashboardRenderBirdnetLogs(payload);
}

function dashboardStartLivePolling() {
  if (dashboardState.livePollHandle !== null) {
    clearInterval(dashboardState.livePollHandle);
  }
  dashboardState.livePollHandle = window.setInterval(async () => {
    try {
      await Promise.all([
        dashboardLoadLiveStatus(),
        dashboardLoadBirdnetLogs(),
      ]);
    } catch (error) {
      dashboardShowError(error);
    }
  }, 1500);
}

async function dashboardLoadRecordings(resetZoom) {
  const previousScrollRatio = dashboardCurrentScrollRatio();
  const params = new URLSearchParams({
    start: new Date(dashboardElements.rangeStart.value).toISOString(),
    end: new Date(dashboardElements.rangeEnd.value).toISOString(),
  });
  const payload = await dashboardFetchJson(`/api/recordings?${params.toString()}`);
  dashboardState.recordings = payload.items || [];
  dashboardState.detections = payload.detections || [];
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
  dashboardRenderRecordingsList();
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
  const liveBirdnetActive = Boolean(service.birdnet_live_analysis_active);

  if (reason === "manual" || reason === "manual-armed") {
    dashboardElements.serviceState.textContent = isRecording
      ? (liveBirdnetActive ? "Manual + BirdNET" : "Manual recording")
      : "Manual ready";
  } else if (reason === "schedule") {
    dashboardElements.serviceState.textContent = isRecording
      ? (liveBirdnetActive ? "Scheduled + BirdNET" : "Scheduled recording")
      : "Watching schedule";
  } else if (reason === "analyzing") {
    dashboardElements.serviceState.textContent = "BirdNET analyzing";
  } else {
    dashboardElements.serviceState.textContent = Boolean(service.started) ? "Idle" : "Recorder disabled";
  }

  dashboardElements.serviceState.classList.toggle("is-recording", isRecording);
  dashboardElements.serviceState.classList.toggle("is-manual", reason === "manual" || reason === "manual-armed");
  dashboardElements.serviceState.classList.toggle("is-processing", reason === "analyzing" || liveBirdnetActive);
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

  dashboardRenderLiveDetections(service);
  dashboardRenderPipeline(service);
  dashboardRenderBirdnetRuntime(service);
  dashboardRenderWaveform(service.waveform_samples || []);
}

function dashboardBuildSpeciesState(service) {
  if (service.species_enabled) {
    return "BirdNET species detection active every 9 seconds during recording";
  }
  if (service.species_provider === "birdnet") {
    return service.species_available === false ? "BirdNET unavailable" : "BirdNET selected";
  }
  return "Species analysis disabled";
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

function dashboardRenderLiveDetections(service) {
  const items = service.live_detections || [];
  const liveWindowSeconds = Number(service.birdnet_live_window_seconds || 9);
  const pendingWindows = Number(service.birdnet_live_pending_windows || 0);
  const completedWindows = Number(service.birdnet_live_completed_windows || 0);

  dashboardElements.liveDetectionsSummary.textContent = service.is_recording
    ? `BirdNET checks each finished ${liveWindowSeconds}-second window while the recording continues. ${completedWindows} window(s) completed, ${pendingWindows} pending.`
    : "BirdNET checks each finished 9-second window while the recording continues.";

  dashboardElements.liveDetectionsList.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state live-detections-empty";
    empty.textContent = service.is_recording
      ? "No live BirdNET detections yet for this recording."
      : "No live BirdNET detections yet.";
    dashboardElements.liveDetectionsList.append(empty);
    return;
  }

  items.forEach((item) => {
    const row = document.createElement("article");
    row.className = "live-detection-item";
    row.innerHTML = `
      <div class="live-detection-main">
        <strong>${item.species_common_name || "Bird detection"}</strong>
        ${item.species_scientific_name ? `<span class="species-scientific">${item.species_scientific_name}</span>` : ""}
        <span>${dashboardFormatDateTime(item.started_at)} - ${dashboardFormatDateTime(item.ended_at)}</span>
      </div>
      <div class="live-detection-meta">
        <span>${Math.round((Number(item.confidence || 0)) * 100)}% confidence</span>
      </div>
    `;
    dashboardElements.liveDetectionsList.append(row);
  });
}

function dashboardBuildActivityDetail(service, activeSchedules) {
  if (service.activity_reason === "analyzing") {
    return service.processing_message || "BirdNET is finishing the last live windows now.";
  }
  if (service.activity_reason === "manual" || service.activity_reason === "manual-armed" || service.manual_mode) {
    return "Manual control overrides the schedule until you press Stop. BirdNET keeps checking finished 9-second windows while recording continues.";
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
  if (reason === "analyzing") {
    return "BirdNET finalizing live windows";
  }
  if (reason === "manual" || reason === "manual-armed" || manualMode) {
    return isRecording ? "Manual mode live" : "Manual mode armed";
  }
  if (reason === "schedule") {
    return isRecording ? "Scheduled recording live" : "Schedule monitoring";
  }
  return "Idle";
}

function dashboardRenderPipeline(service) {
  const processingStage = service.processing_stage || "idle";
  const lastSummary = service.last_processing_summary || "No BirdNET analysis has completed yet.";
  const clipCount = Number(service.last_clip_count || 0);
  const detectionCount = Number(service.last_detection_count || 0);
  const detectedSpecies = (service.last_detected_species || []).join(", ");
  const pendingLiveWindows = Number(service.birdnet_live_pending_windows || 0);
  const liveDetectionCount = Number(service.live_detection_count || 0);
  const liveDetectedSpecies = (service.live_detected_species || []).join(", ");

  dashboardElements.pipelineModeNote.textContent = service.species_enabled
    ? `BirdNET checks each finished ${service.birdnet_live_window_seconds || 9}-second window while the recording keeps running.`
    : "BirdNET matching mode is unavailable.";

  dashboardSetPipelineCard(
    dashboardElements.pipelineRecordingCard,
    dashboardElements.pipelineRecordingState,
    dashboardElements.pipelineRecordingDetail,
    service.is_recording ? "Capturing audio" : "Waiting for next recording",
    service.is_recording
      ? "The microphone is recording right now."
      : (service.current_device_name || "BirdNET starts once recording begins."),
    service.is_recording,
    false,
  );

  const birdnetUnavailable = service.species_provider === "birdnet" && service.species_available === false;
  const birdnetActive = Boolean(service.birdnet_live_analysis_active) || processingStage === "analyzing";
  dashboardSetPipelineCard(
    dashboardElements.pipelineBirdnetCard,
    dashboardElements.pipelineBirdnetState,
    dashboardElements.pipelineBirdnetDetail,
    birdnetUnavailable ? "Unavailable" : (birdnetActive ? "Analyzing live window" : "Waiting for next window"),
    birdnetUnavailable
      ? (service.species_error || "BirdNET could not be loaded.")
      : (
        birdnetActive
          ? (service.processing_message || "BirdNET is checking the newest finished 9-second window.")
          : "BirdNET analyzes each finished 9-second window while recording keeps going."
      ),
    birdnetActive,
    birdnetUnavailable,
  );

  const clipsActive = processingStage === "extracting-clips";
  dashboardSetPipelineCard(
    dashboardElements.pipelineClipsCard,
    dashboardElements.pipelineClipsState,
    dashboardElements.pipelineClipsDetail,
    clipsActive ? "Saving clips" : (clipCount > 0 ? `Saved ${clipCount} clip(s)` : "Waiting for detections"),
    clipsActive
      ? (service.processing_message || "Writing separate WAV files for each detected bird occurrence.")
      : (clipCount > 0 ? "Each detected bird occurrence was saved as its own audio file." : "When BirdNET finds birds, each occurrence gets its own clip file."),
    clipsActive,
    false,
  );

  dashboardSetPipelineCard(
    dashboardElements.pipelineResultCard,
    dashboardElements.pipelineResultState,
    dashboardElements.pipelineResultDetail,
    liveDetectionCount > 0
      ? `${liveDetectionCount} live detection(s)`
      : (detectionCount > 0 ? `${detectionCount} saved detection(s)` : "No detections yet"),
    liveDetectionCount > 0 && liveDetectedSpecies
      ? `Live updates so far: ${liveDetectedSpecies}. Pending BirdNET windows: ${pendingLiveWindows}.`
      : (detectionCount > 0 && detectedSpecies ? `${lastSummary} Species: ${detectedSpecies}.` : lastSummary),
    false,
    false,
  );
}

function dashboardSetPipelineCard(card, stateElement, detailElement, stateText, detailText, isActive, isAlert) {
  stateElement.textContent = stateText;
  detailElement.textContent = detailText;
  card.classList.toggle("is-active", Boolean(isActive));
  card.classList.toggle("is-alert", Boolean(isAlert));
}

function dashboardRenderBirdnetRuntime(service) {
  const runtime = service.birdnet_runtime_details || {};
  const packages = runtime.packages || {};
  const available = runtime.available === true || service.species_available === true;
  const disabled = service.species_provider === "disabled" && !available;
  const installedState = available
    ? "Ready"
    : (disabled ? "Disabled" : "Unavailable");
  const backend = runtime.runtime_backend || "unknown";
  const lastFinishedAt = service.birdnet_last_analysis_finished_at;
  const lastDuration = service.birdnet_last_analysis_duration_seconds;
  const lastTarget = service.birdnet_last_analysis_target;
  const packageSummary = [
    `birdnetlib ${packages["birdnetlib"] || "missing"}`,
    `librosa ${packages["librosa"] || "missing"}`,
    `tensorflow ${packages["tensorflow"] || "missing"}`,
    `tflite-runtime ${packages["tflite-runtime"] || "missing"}`,
  ].join(" | ");

  dashboardElements.birdnetInstalledState.textContent = installedState;
  dashboardElements.birdnetBackendState.textContent = backend;
  dashboardElements.birdnetLastAnalysisState.textContent = lastFinishedAt
    ? `${new Date(lastFinishedAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}${lastDuration != null ? ` (${Number(lastDuration).toFixed(1)} s)` : ""}`
    : "No completed run";
  dashboardElements.birdnetLastTargetState.textContent = lastTarget ? dashboardShortenPath(lastTarget) : "None yet";
  dashboardElements.birdnetPackageSummary.textContent = `Packages: ${packageSummary}. Analysis mode: ${runtime.analysis_mode || "post-recording"}.`;
  dashboardElements.birdnetLogPath.textContent = service.birdnet_log_file
    ? `BirdNET log: ${service.birdnet_log_file}${service.app_log_file ? ` | App log: ${service.app_log_file}` : ""}`
    : "BirdNET log file path is not available yet.";
  dashboardShowLogStatus(dashboardState.birdnetLogStatusMessage, dashboardState.birdnetLogStatusTone);

  if (available) {
    dashboardElements.birdnetRuntimeSummary.textContent = service.birdnet_live_analysis_active
      ? "BirdNET is installed and actively analyzing the newest finished 9-second window right now."
      : "BirdNET is installed. It analyzes finished 9-second windows during recording and logs every analysis step below.";
    return;
  }

  const reason = runtime.reason || service.species_error || "BirdNET is not available in the current runtime.";
  dashboardElements.birdnetRuntimeSummary.textContent = `BirdNET is not ready. ${reason}`;
}

function dashboardRenderBirdnetLogs(payload) {
  const items = payload.items || [];
  const previousDistanceFromBottom = dashboardLogConsoleDistanceFromBottom();
  const shouldStickToBottom = dashboardState.birdnetLogAutoFollow || previousDistanceFromBottom <= 20;
  dashboardElements.birdnetLogConsole.innerHTML = "";

  if (!items.length) {
    dashboardElements.birdnetLogConsole.innerHTML = `<div class="birdnet-log-empty">No BirdNET log entries yet.</div>`;
    dashboardState.birdnetLogAutoFollow = true;
    return;
  }

  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = `birdnet-log-line is-${String(item.level || "info").toLowerCase()}`;

    const time = document.createElement("span");
    time.className = "birdnet-log-time";
    time.textContent = dashboardFormatLogTimestamp(item.timestamp);

    const level = document.createElement("span");
    level.className = "birdnet-log-level";
    level.textContent = item.level || "INFO";

    const logger = document.createElement("span");
    logger.className = "birdnet-log-logger";
    logger.textContent = item.thread ? `${item.logger || "birdnet"} @ ${item.thread}` : (item.logger || "birdnet");

    const message = document.createElement("span");
    message.className = "birdnet-log-message";
    message.textContent = item.message || "";

    const copyButton = document.createElement("button");
    copyButton.type = "button";
    copyButton.className = "log-copy-button secondary-button";
    copyButton.textContent = "Copy";
    copyButton.addEventListener("click", async () => {
      try {
        await dashboardCopyLogEntry(item, copyButton);
      } catch (error) {
        dashboardShowError(error);
      }
    });

    row.append(time, level, logger, message, copyButton);
    dashboardElements.birdnetLogConsole.append(row);
  });

  if (shouldStickToBottom) {
    dashboardElements.birdnetLogConsole.scrollTop = dashboardElements.birdnetLogConsole.scrollHeight;
    dashboardState.birdnetLogAutoFollow = true;
    return;
  }

  dashboardElements.birdnetLogConsole.scrollTop = Math.max(
    dashboardElements.birdnetLogConsole.scrollHeight
      - dashboardElements.birdnetLogConsole.clientHeight
      - previousDistanceFromBottom,
    0,
  );
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

  const totalDetections = dashboardState.detections.length;
  dashboardElements.timelineSummary.textContent = `${dashboardState.recordings.length} recording(s), ${totalDetections} BirdNET detection(s), and ${dashboardState.speciesEvents.length} merged event(s) between ${dashboardFormatDateTime(dashboardState.range.start)} and ${dashboardFormatDateTime(dashboardState.range.end)}.`;
  dashboardUpdateZoomLabel();

  if (!dashboardState.recordings.length && !dashboardState.detections.length) {
    dashboardElements.timelineEmpty.style.display = "block";
    dashboardElements.timelineEmpty.textContent = "No recordings or BirdNET detections were found in this time span.";
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
  const detectionRows = dashboardBuildDetectionRows(width);
  const detectionRowCount = detectionRows.length
    ? Math.max(...detectionRows.map((item) => item.rowIndex)) + 1
    : 1;
  track.style.minHeight = `${Math.max(220, 64 + (detectionRowCount * 40) + 120)}px`;

  const recordingLane = document.createElement("div");
  recordingLane.className = "recording-lane";
  dashboardState.recordings.forEach((recording) => {
    recordingLane.append(dashboardBuildRecordingBlock(recording, width));
  });
  track.append(recordingLane);

  const detectionLane = document.createElement("div");
  detectionLane.className = "species-lane";
  detectionRows.forEach((detectionRow) => {
    detectionLane.append(dashboardBuildDetectionChip(detectionRow, width));
  });
  track.append(detectionLane);

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
    : "No BirdNET detections";
  return `${localStart.toLocaleTimeString()} - ${localEnd.toLocaleTimeString()} | ${summary}`;
}

function dashboardBuildDetectionRows(width) {
  const rowEndOffsets = [];
  return dashboardState.detections.map((detection) => {
    const start = new Date(detection.started_at);
    const end = new Date(detection.ended_at);
    const left = dashboardRangeRatio(start) * width;
    const right = dashboardRangeRatio(end) * width;
    const label = `${detection.species_common_name} ${Math.round((detection.species_score || detection.confidence || 0) * 100)}%`;
    const naturalWidth = Math.max(right - left, Math.max(150, (label.length * 7)));
    const chipWidth = Math.min(Math.max(naturalWidth, 150), 280);

    let rowIndex = rowEndOffsets.findIndex((offset) => left > (offset + 12));
    if (rowIndex === -1) {
      rowIndex = rowEndOffsets.length;
      rowEndOffsets.push(left + chipWidth);
    } else {
      rowEndOffsets[rowIndex] = left + chipWidth;
    }

    return { detection, rowIndex, chipWidth, left };
  });
}

function dashboardBuildDetectionChip(detectionRow, width) {
  const detection = detectionRow.detection;
  const chip = document.createElement(detection.clip_url ? "a" : "div");
  chip.className = "species-event-chip";
  chip.style.top = `${10 + (detectionRow.rowIndex * 38)}px`;

  const start = new Date(detection.started_at);
  const left = dashboardRangeRatio(start) * width;
  const chipWidth = detectionRow.chipWidth || 160;
  const maxLeft = Math.max(width - chipWidth - 6, 0);

  chip.style.left = `${Math.min(left, maxLeft)}px`;
  chip.style.width = `${chipWidth}px`;
  chip.title = dashboardBuildDetectionTitle(detection);
  if (detection.clip_url) {
    chip.href = detection.clip_url;
    chip.target = "_blank";
    chip.rel = "noopener";
  }
  chip.innerHTML = `
    <strong>${detection.species_common_name}</strong>
    <span>${Math.round(((detection.species_score != null ? detection.species_score : detection.confidence) || 0) * 100)}%</span>
  `;
  return chip;
}

function dashboardBuildDetectionTitle(detection) {
  const scientific = detection.species_scientific_name ? ` (${detection.species_scientific_name})` : "";
  const clipText = detection.clip_url ? "Open saved occurrence clip" : "No separate clip file";
  return `${detection.species_common_name}${scientific} | ${dashboardFormatDateTime(detection.started_at)} - ${dashboardFormatDateTime(detection.ended_at)} | confidence ${Math.round(((detection.species_score != null ? detection.species_score : detection.confidence) || 0) * 100)}% | ${clipText}`;
}

function dashboardRenderStatistics() {
  dashboardElements.statsGrid.innerHTML = "";
  dashboardElements.speciesStatsList.innerHTML = "";

  if (!dashboardState.range) {
    dashboardElements.statsSummary.textContent = "No species events loaded yet.";
    return;
  }

  const mergedEvents = dashboardState.speciesEvents.length;
  const statsItems = dashboardEffectiveSpeciesStats();
  const speciesCount = statsItems.length;
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

  if (!statsItems.length) {
    dashboardElements.speciesStatsList.innerHTML = `<div class="empty-state">No species were identified in this time span.</div>`;
    return;
  }

  statsItems.forEach((item) => {
    const row = document.createElement("details");
    row.className = "species-stat-row";
    const scientific = item.species_scientific_name ? `<span class="species-scientific">${item.species_scientific_name}</span>` : "";
    row.innerHTML = `
      <summary class="species-stat-summary">
        <div class="species-stat-main">
          <strong>${item.species_common_name}</strong>
          ${scientific}
        </div>
        <div class="species-stat-summary-metrics">
          <span>${item.event_count} event(s)</span>
          <span>${Math.round((item.best_confidence || 0) * 100)}% best</span>
        </div>
      </summary>
      <div class="species-stat-body">
        <div class="species-stat-metrics">
          <span>${item.event_count} event(s)</span>
          <span>${item.detection_count || item.event_count} detection(s)</span>
          <span>${Math.round((item.average_confidence || 0) * 100)}% avg</span>
          <span>${Math.round((item.best_confidence || 0) * 100)}% best</span>
          <span>${item.last_seen_at ? `last ${new Date(item.last_seen_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}` : "-"}</span>
        </div>
      </div>
    `;
    dashboardElements.speciesStatsList.append(row);
  });
}

function dashboardRenderRecordingsList() {
  dashboardElements.recordingsList.innerHTML = "";

  if (!dashboardState.range) {
    dashboardElements.recordingsSummary.textContent = "No recordings loaded yet.";
    dashboardElements.recordingsList.innerHTML = `<div class="empty-state">Load a time range to see recordings and BirdNET clips.</div>`;
    return;
  }

  const recordings = [...dashboardState.recordings].sort(
    (left, right) => new Date(right.started_at) - new Date(left.started_at),
  );
  dashboardElements.recordingsSummary.textContent = `${recordings.length} recording(s) and ${dashboardState.detections.length} detected bird clip(s) in the selected range.`;

  if (!recordings.length) {
    dashboardElements.recordingsList.innerHTML = `<div class="empty-state">No recordings were found in this time span.</div>`;
    return;
  }

  recordings.forEach((recording) => {
    dashboardElements.recordingsList.append(dashboardBuildRecordingAccordion(recording));
  });
}

function dashboardBuildRecordingAccordion(recording) {
  const wrapper = document.createElement("details");
  wrapper.className = "recording-item";

  const summary = document.createElement("summary");
  summary.className = "recording-item-summary";

  const startedAt = new Date(recording.started_at);
  const endedAt = new Date(recording.ended_at);
  const detections = recording.detections || [];
  const speciesBadges = dashboardBuildRecordingSpeciesBadges(detections);
  const detectionCount = detections.length;

  summary.innerHTML = `
    <div class="recording-item-main">
      <strong>${startedAt.toLocaleString()}</strong>
      <span>${dashboardFormatDuration(recording.duration_seconds || ((endedAt - startedAt) / 1000))}</span>
      <span>${recording.device_name || "Unknown microphone"}</span>
    </div>
    <div class="recording-item-meta">
      <span class="recording-count-chip">${detectionCount} bird event(s)</span>
      <span class="recording-count-chip">${Math.round((recording.size_bytes || 0) / 1024)} KB</span>
    </div>
  `;

  const speciesStrip = document.createElement("div");
  speciesStrip.className = "recording-species-strip";
  speciesStrip.innerHTML = speciesBadges || `<span class="recording-species-chip is-muted">No BirdNET detections</span>`;

  const content = document.createElement("div");
  content.className = "recording-item-body";

  const actionRow = document.createElement("div");
  actionRow.className = "recording-item-actions";
  actionRow.innerHTML = `
    <a class="secondary-button button-link" href="${recording.audio_url}" download>Download recording</a>
    <button type="button" class="danger-button">Delete recording</button>
  `;

  const deleteButton = actionRow.querySelector("button");
  deleteButton.addEventListener("click", async () => {
    try {
      await dashboardDeleteRecording(recording.id, deleteButton);
    } catch (error) {
      dashboardShowError(error);
    }
  });

  const meta = document.createElement("div");
  meta.className = "recording-item-details";
  meta.innerHTML = `
    <span>Started: ${dashboardFormatDateTime(recording.started_at)}</span>
    <span>Ended: ${dashboardFormatDateTime(recording.ended_at)}</span>
    <span>Sample rate: ${recording.sample_rate} Hz</span>
    <span>Channels: ${recording.channels}</span>
    <span>Peak: ${Math.round((recording.peak_amplitude || 0) * 100)}%</span>
  `;

  const detectionsList = document.createElement("div");
  detectionsList.className = "recording-detections-list";
  if (!detections.length) {
    detectionsList.innerHTML = `<div class="empty-state recording-empty-state">No BirdNET clips were saved for this recording.</div>`;
  } else {
    detections.forEach((detection) => {
      detectionsList.append(dashboardBuildRecordingDetectionItem(detection));
    });
  }

  content.append(actionRow, meta, detectionsList);
  wrapper.append(summary, speciesStrip, content);
  return wrapper;
}

function dashboardBuildRecordingSpeciesBadges(detections) {
  const seen = new Set();
  return detections
    .filter((detection) => detection.species_common_name)
    .filter((detection) => {
      if (seen.has(detection.species_common_name)) {
        return false;
      }
      seen.add(detection.species_common_name);
      return true;
    })
    .map((detection) => {
      const confidence = detection.species_score != null ? detection.species_score : detection.confidence;
      return `<span class="recording-species-chip">${detection.species_common_name} ${Math.round((confidence || 0) * 100)}%</span>`;
    })
    .join("");
}

function dashboardBuildRecordingDetectionItem(detection) {
  const item = document.createElement("article");
  item.className = "recording-detection-item";
  const confidence = detection.species_score != null ? detection.species_score : detection.confidence;
  const scientific = detection.species_scientific_name ? `<span class="species-scientific">${detection.species_scientific_name}</span>` : "";
  const clipAction = detection.clip_url
    ? `<a class="secondary-button button-link" href="${detection.clip_url}" download>Download clip</a>`
    : `<span class="recording-clip-missing">No clip file</span>`;

  item.innerHTML = `
    <div class="recording-detection-main">
      <strong>${detection.species_common_name || "Bird detection"}</strong>
      ${scientific}
      <span>${dashboardFormatDateTime(detection.started_at)} - ${dashboardFormatDateTime(detection.ended_at)}</span>
    </div>
    <div class="recording-detection-meta">
      <span>${Math.round((confidence || 0) * 100)}% confidence</span>
      ${clipAction}
    </div>
  `;
  return item;
}

async function dashboardDeleteRecording(recordingId, button) {
  const confirmed = window.confirm("Delete this recording and all saved BirdNET clips linked to it?");
  if (!confirmed) {
    return;
  }

  const originalLabel = button.textContent;
  button.disabled = true;
  button.textContent = "Deleting...";
  try {
    await dashboardFetchJson(`/api/recordings/${recordingId}`, { method: "DELETE" });
    await Promise.all([
      dashboardLoadStatus(),
      dashboardLoadRecordings(false),
    ]);
  } finally {
    button.disabled = false;
    button.textContent = originalLabel;
  }
}

function dashboardEffectiveSpeciesStats() {
  if (dashboardState.speciesStats.length) {
    return dashboardState.speciesStats;
  }
  return dashboardBuildSpeciesStatsFromEvents(dashboardState.speciesEvents);
}

function dashboardBuildSpeciesStatsFromEvents(events) {
  const buckets = new Map();

  events.forEach((event) => {
    const commonName = event.species_common_name || "Unknown species";
    const scientificName = event.species_scientific_name || null;
    const key = `${commonName}::${scientificName || ""}`;
    const eventConfidence = Number(
      event.average_confidence != null ? event.average_confidence : (event.confidence || 0),
    );
    const detectionCount = Number(event.detection_count || 1);
    const endedAt = event.ended_at || null;

    if (!buckets.has(key)) {
      buckets.set(key, {
        species_common_name: commonName,
        species_scientific_name: scientificName,
        event_count: 0,
        detection_count: 0,
        average_confidence_total: 0,
        best_confidence: 0,
        last_seen_at: endedAt,
      });
    }

    const bucket = buckets.get(key);
    bucket.event_count += 1;
    bucket.detection_count += detectionCount;
    bucket.average_confidence_total += eventConfidence;
    bucket.best_confidence = Math.max(bucket.best_confidence, Number(event.confidence || eventConfidence || 0));
    if (endedAt && (!bucket.last_seen_at || new Date(endedAt) > new Date(bucket.last_seen_at))) {
      bucket.last_seen_at = endedAt;
    }
  });

  return Array.from(buckets.values())
    .map((bucket) => ({
      species_common_name: bucket.species_common_name,
      species_scientific_name: bucket.species_scientific_name,
      event_count: bucket.event_count,
      detection_count: bucket.detection_count,
      average_confidence: bucket.average_confidence_total / Math.max(bucket.event_count, 1),
      best_confidence: bucket.best_confidence,
      last_seen_at: bucket.last_seen_at,
    }))
    .sort((left, right) => {
      if (right.event_count !== left.event_count) {
        return right.event_count - left.event_count;
      }
      if (right.average_confidence !== left.average_confidence) {
        return right.average_confidence - left.average_confidence;
      }
      return String(left.species_common_name).localeCompare(String(right.species_common_name));
    });
}

function dashboardBuildDetectionInline(detection) {
  const label = detection.species_common_name || "Bird";
  const confidence = detection.species_score != null ? detection.species_score : detection.confidence;
  return `${new Date(detection.started_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} ${label} ${Math.round((confidence || 0) * 100)}%`;
}

function dashboardFormatDuration(seconds) {
  const totalSeconds = Math.max(0, Math.round(Number(seconds || 0)));
  const minutes = Math.floor(totalSeconds / 60);
  const remainingSeconds = totalSeconds % 60;
  if (minutes <= 0) {
    return `${remainingSeconds}s`;
  }
  return `${minutes}m ${`${remainingSeconds}`.padStart(2, "0")}s`;
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

function dashboardFormatLogTimestamp(value) {
  if (!value) {
    return "--:--:--";
  }
  return new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function dashboardLogConsoleDistanceFromBottom() {
  const element = dashboardElements.birdnetLogConsole;
  return Math.max(element.scrollHeight - element.scrollTop - element.clientHeight, 0);
}

function dashboardLogConsoleIsNearBottom() {
  return dashboardLogConsoleDistanceFromBottom() <= 20;
}

async function dashboardCopyLogEntry(item, button) {
  const logger = item.thread ? `${item.logger || "birdnet"} @ ${item.thread}` : (item.logger || "birdnet");
  const text = `${dashboardFormatLogTimestamp(item.timestamp)} ${item.level || "INFO"} ${logger} ${item.message || ""}`.trim();
  await dashboardWriteClipboard(text);
  const originalLabel = button.textContent;
  button.textContent = "Copied";
  window.setTimeout(() => {
    button.textContent = originalLabel;
  }, 1200);
}

async function dashboardWriteClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const helper = document.createElement("textarea");
  helper.value = text;
  helper.setAttribute("readonly", "readonly");
  helper.style.position = "absolute";
  helper.style.left = "-9999px";
  document.body.append(helper);
  helper.select();
  document.execCommand("copy");
  helper.remove();
}

function dashboardShortenPath(value) {
  const normalized = String(value || "");
  const parts = normalized.split(/[\\/]/).filter(Boolean);
  if (parts.length <= 3) {
    return normalized;
  }
  return `.../${parts.slice(-3).join("/")}`;
}

async function initDashboard() {
  dashboardSetDefaultRange();
  dashboardBindEvents();
  dashboardRenderWaveform(new Array(120).fill(0));
  try {
    await Promise.all([
      dashboardLoadStatus(),
      dashboardLoadRecordings(true),
      dashboardLoadBirdnetLogs(),
    ]);
    dashboardStartLivePolling();
  } catch (error) {
    dashboardShowError(error);
  }
}

document.addEventListener("DOMContentLoaded", initDashboard);
