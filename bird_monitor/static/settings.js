const weekdayLabels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

const settingsState = {
  devices: [],
  schedules: [],
  settings: null,
  status: null,
  selectedDays: new Set([0, 1, 2, 3, 4, 5, 6]),
  lastResolvedAddress: "",
};

const settingsElements = {
  settingsForm: document.querySelector("#settings-form"),
  deviceIndex: document.querySelector("#device-index"),
  deviceName: document.querySelector("#device-name"),
  sampleRate: document.querySelector("#sample-rate"),
  channels: document.querySelector("#channels"),
  segmentSeconds: document.querySelector("#segment-seconds"),
  speciesProvider: document.querySelector("#species-provider"),
  speciesMinConfidence: document.querySelector("#species-min-confidence"),
  locationName: document.querySelector("#location-name"),
  resolveAddressButton: document.querySelector("#resolve-address-button"),
  latitude: document.querySelector("#latitude"),
  longitude: document.querySelector("#longitude"),
  devicesNote: document.querySelector("#devices-note"),
  locationNote: document.querySelector("#location-note"),
  speciesStatusNote: document.querySelector("#species-status-note"),
  scheduleForm: document.querySelector("#schedule-form"),
  scheduleName: document.querySelector("#schedule-name"),
  scheduleStart: document.querySelector("#schedule-start"),
  scheduleEnd: document.querySelector("#schedule-end"),
  scheduleEnabled: document.querySelector("#schedule-enabled"),
  weekdayPicker: document.querySelector("#weekday-picker"),
  scheduleList: document.querySelector("#schedule-list"),
};

function settingsFetchJson(url, options = {}) {
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

function settingsShowError(error) {
  settingsElements.devicesNote.textContent = error.message || String(error);
}

function settingsBuildWeekdayPicker() {
  weekdayLabels.forEach((label, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "weekday-chip is-selected";
    button.textContent = label;
    button.addEventListener("click", () => {
      if (settingsState.selectedDays.has(index)) {
        settingsState.selectedDays.delete(index);
      } else {
        settingsState.selectedDays.add(index);
      }
      button.classList.toggle("is-selected", settingsState.selectedDays.has(index));
    });
    settingsElements.weekdayPicker.append(button);
  });
}

function settingsBindEvents() {
  settingsElements.deviceIndex.addEventListener("change", () => {
    settingsRenderCompatibilityOptions();
  });

  settingsElements.speciesProvider.addEventListener("change", () => {
    settingsRenderSpeciesStatus();
  });

  settingsElements.locationName.addEventListener("input", () => {
    settingsInvalidateResolvedAddress();
    settingsRenderSpeciesStatus();
  });

  settingsElements.locationName.addEventListener("blur", async () => {
    if (!settingsNeedsGeocode()) {
      return;
    }
    try {
      await settingsResolveAddress();
    } catch (error) {
      settingsElements.locationNote.textContent = error.message || String(error);
    }
  });

  settingsElements.resolveAddressButton.addEventListener("click", async () => {
    try {
      await settingsResolveAddress(true);
    } catch (error) {
      settingsElements.locationNote.textContent = error.message || String(error);
    }
  });

  settingsElements.settingsForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const selectedIndex = settingsElements.deviceIndex.value;
      const needsGeocode = settingsNeedsGeocode();
      if (needsGeocode) {
        await settingsResolveAddress(true);
      }

      await settingsFetchJson("/api/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          device_index: selectedIndex === "" ? null : Number(selectedIndex),
          device_name: settingsElements.deviceName.value.trim(),
          sample_rate: Number(settingsElements.sampleRate.value),
          channels: Number(settingsElements.channels.value),
          segment_seconds: Number(settingsElements.segmentSeconds.value),
          species_provider: settingsElements.speciesProvider.value,
          species_min_confidence: Number(settingsElements.speciesMinConfidence.value),
          location_name: settingsElements.locationName.value.trim(),
          latitude: settingsElements.latitude.value === "" ? null : Number(settingsElements.latitude.value),
          longitude: settingsElements.longitude.value === "" ? null : Number(settingsElements.longitude.value),
          auto_geocode: false,
        }),
      });
      await settingsRefreshAll();
      settingsElements.devicesNote.textContent = "Recorder settings saved.";
    } catch (error) {
      settingsShowError(error);
    }
  });

  settingsElements.scheduleForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await settingsFetchJson("/api/schedules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: settingsElements.scheduleName.value.trim(),
          days_of_week: Array.from(settingsState.selectedDays).sort((a, b) => a - b),
          start_time: settingsElements.scheduleStart.value,
          end_time: settingsElements.scheduleEnd.value,
          enabled: settingsElements.scheduleEnabled.checked,
        }),
      });
      settingsElements.scheduleName.value = "";
      await settingsLoadSchedules();
    } catch (error) {
      settingsShowError(error);
    }
  });
}

async function settingsRefreshAll() {
  await settingsLoadDevices();
  await settingsLoadSettings();
  await settingsLoadStatus();
  await settingsLoadSchedules();
}

async function settingsLoadDevices() {
  const payload = await fetch("/api/devices");
  const data = await payload.json();
  settingsState.devices = data.items || [];
  settingsRenderDevices(data.error || "");
}

async function settingsLoadSettings() {
  settingsState.settings = await settingsFetchJson("/api/settings");
  settingsRenderSettings();
}

async function settingsLoadSchedules() {
  const payload = await settingsFetchJson("/api/schedules");
  settingsState.schedules = payload.items;
  settingsRenderSchedules();
}

async function settingsLoadStatus() {
  const payload = await settingsFetchJson("/api/status");
  settingsState.status = payload.service || null;
  settingsRenderSpeciesStatus();
}

function settingsRenderDevices(errorText) {
  settingsElements.deviceIndex.innerHTML = "";
  const autoOption = document.createElement("option");
  autoOption.value = "";
  autoOption.textContent = "Auto select best input device";
  settingsElements.deviceIndex.append(autoOption);

  settingsState.devices.forEach((device) => {
    const option = document.createElement("option");
    option.value = `${device.index}`;
    option.textContent = `${device.name}`;
    settingsElements.deviceIndex.append(option);
  });

  settingsElements.devicesNote.textContent = errorText || (
    settingsState.devices.length
      ? "Only supported sample rates and channel counts are shown for the selected microphone."
      : "No input devices detected yet."
  );
}

function settingsRenderSettings() {
  if (!settingsState.settings) {
    return;
  }

  settingsElements.deviceIndex.value = settingsState.settings.device_index == null ? "" : `${settingsState.settings.device_index}`;
  settingsElements.deviceName.value = settingsState.settings.device_name || "";
  settingsElements.segmentSeconds.value = `${settingsState.settings.segment_seconds}`;
  settingsElements.speciesProvider.value = settingsState.settings.species_provider || "disabled";
  settingsElements.speciesMinConfidence.value = `${settingsState.settings.species_min_confidence ?? 0.35}`;
  settingsElements.locationName.value = settingsState.settings.location_name || "";
  settingsElements.latitude.value = settingsState.settings.latitude ?? "";
  settingsElements.longitude.value = settingsState.settings.longitude ?? "";
  settingsState.lastResolvedAddress = settingsState.settings.location_name || "";
  settingsRenderCompatibilityOptions();
  settingsRenderLocationNote();
  settingsRenderSpeciesStatus();
}

function settingsRenderCompatibilityOptions() {
  const selectedIndex = settingsElements.deviceIndex.value;
  const selectedDevice = settingsState.devices.find((device) => `${device.index}` === selectedIndex) || settingsState.devices[0];
  const supportedRates = selectedDevice?.supported_sample_rates?.length ? selectedDevice.supported_sample_rates : [32000];
  const supportedChannels = selectedDevice?.supported_channels?.length ? selectedDevice.supported_channels : [1];

  settingsPopulateSelect(settingsElements.sampleRate, supportedRates, settingsState.settings?.sample_rate);
  settingsPopulateSelect(settingsElements.channels, supportedChannels, settingsState.settings?.channels);
}

function settingsPopulateSelect(selectElement, values, preferredValue) {
  selectElement.innerHTML = "";
  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = `${value}`;
    option.textContent = `${value}`;
    selectElement.append(option);
  });

  const chosenValue = values.includes(preferredValue) ? preferredValue : values[0];
  if (chosenValue != null) {
    selectElement.value = `${chosenValue}`;
  }
}

function settingsNeedsGeocode() {
  const query = settingsElements.locationName.value.trim();
  return Boolean(
    query && (
      query !== settingsState.lastResolvedAddress
      || settingsElements.latitude.value === ""
      || settingsElements.longitude.value === ""
    )
  );
}

function settingsInvalidateResolvedAddress() {
  if (!settingsElements.locationName.value.trim()) {
    settingsState.lastResolvedAddress = "";
    settingsElements.latitude.value = "";
    settingsElements.longitude.value = "";
    settingsRenderLocationNote();
    return;
  }

  if (settingsElements.locationName.value.trim() !== settingsState.lastResolvedAddress) {
    settingsElements.locationNote.textContent = "Address changed. Coordinates will be refreshed automatically when you save.";
  }
}

async function settingsResolveAddress(force = false) {
  const query = settingsElements.locationName.value.trim();
  if (!query) {
    settingsState.lastResolvedAddress = "";
    settingsRenderLocationNote();
    return null;
  }

  if (!force && !settingsNeedsGeocode()) {
    return {
      display_name: settingsElements.locationName.value.trim(),
      latitude: Number(settingsElements.latitude.value),
      longitude: Number(settingsElements.longitude.value),
    };
  }

  settingsElements.locationNote.textContent = "Resolving address...";
  const payload = await settingsFetchJson("/api/geocode", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });

  settingsElements.locationName.value = payload.display_name;
  settingsElements.latitude.value = Number(payload.latitude).toFixed(5);
  settingsElements.longitude.value = Number(payload.longitude).toFixed(5);
  settingsState.lastResolvedAddress = payload.display_name;
  settingsRenderLocationNote();
  return payload;
}

function settingsRenderLocationNote() {
  const query = settingsElements.locationName.value.trim();
  const latitude = settingsElements.latitude.value;
  const longitude = settingsElements.longitude.value;

  if (!query) {
    settingsElements.locationNote.textContent = "Enter an address or place name and the coordinates will be filled automatically.";
    return;
  }

  if (latitude !== "" && longitude !== "") {
    settingsElements.locationNote.textContent = `Resolved location: ${query} (${latitude}, ${longitude}).`;
    return;
  }

  settingsElements.locationNote.textContent = "Enter an address or place name and the coordinates will be filled automatically.";
}

function settingsRenderSpeciesStatus() {
  const provider = settingsElements.speciesProvider.value || settingsState.settings?.species_provider || "disabled";
  const status = settingsState.status || {};

  if (provider === "birdnet" && status.species_enabled) {
    const locationText = settingsElements.locationName.value.trim()
      ? ` for ${settingsElements.locationName.value.trim()}`
      : "";
    const runtimeNote = status.species_error ? ` Last analysis problem: ${status.species_error}` : "";
    settingsElements.speciesStatusNote.textContent = `BirdNET is active${locationText}. It analyzes each finished 9-second window while the recording continues and uses the configured coordinates plus recording date.${runtimeNote}`;
    return;
  }

  if (provider === "birdnet" && status.species_available === false) {
    const reason = status.species_error ? ` ${status.species_error}` : "";
    settingsElements.speciesStatusNote.textContent = `BirdNET is selected, but the server does not currently have its runtime dependencies available.${reason} Recording still works, but species labels will stay unavailable until BirdNET is installed and the service is restarted.`;
    return;
  }

  if (provider === "birdnet") {
    settingsElements.speciesStatusNote.textContent = "BirdNET is selected. It will analyze each finished 9-second window while recording continues, using the configured coordinates and the recording date.";
    return;
  }

  settingsElements.speciesStatusNote.textContent = "Species analysis is disabled. The timeline will not show bird detections until BirdNET is enabled again.";
}

function settingsRenderSchedules() {
  settingsElements.scheduleList.innerHTML = "";
  if (!settingsState.schedules.length) {
    settingsElements.scheduleList.innerHTML = `<div class="empty-state">No schedules yet. Add one above.</div>`;
    return;
  }

  settingsState.schedules.forEach((schedule) => {
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

    settingsElements.scheduleList.append(wrapper);
  });

  settingsElements.scheduleList.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", async () => {
      const scheduleId = Number(button.dataset.id);
      const schedule = settingsState.schedules.find((item) => item.id === scheduleId);
      if (!schedule) {
        return;
      }

      try {
        if (button.dataset.action === "delete") {
          await settingsFetchJson(`/api/schedules/${scheduleId}`, { method: "DELETE" });
        } else {
          await settingsFetchJson(`/api/schedules/${scheduleId}`, {
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
        }
        await settingsLoadSchedules();
      } catch (error) {
        settingsShowError(error);
      }
    });
  });
}

async function initSettingsPage() {
  settingsBuildWeekdayPicker();
  settingsBindEvents();
  settingsElements.scheduleStart.value = "05:00";
  settingsElements.scheduleEnd.value = "08:00";
  try {
    await settingsRefreshAll();
  } catch (error) {
    settingsShowError(error);
  }
}

document.addEventListener("DOMContentLoaded", initSettingsPage);
