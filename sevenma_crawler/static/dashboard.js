const config = window.__DASHBOARD_CONFIG__;

let map = null;
let pointOverlays = [];
let vehicleOverlays = [];
let refreshTimer = null;
let pointNameById = new Map();
let vehicleInfoWindow = null;

function formatTime(value) {
  if (!value) {
    return "-";
  }
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

function safeText(value) {
  return value ?? "-";
}

function escapeHtml(value) {
  return String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

async function loadAmap() {
  if (window.AMap) {
    return;
  }

  window._AMapSecurityConfig = {
    securityJsCode: config.amapSecurityJsCode,
  };

  await new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = `https://webapi.amap.com/maps?v=2.0&key=${encodeURIComponent(config.amapKey)}`;
    script.async = true;
    script.onload = resolve;
    script.onerror = () => reject(new Error("failed to load AMap JS API"));
    document.head.appendChild(script);
  });
}

function computeCenter(points, vehicles) {
  const source = vehicles.length > 0 ? vehicles : points;
  if (!source.length) {
    return [118.715, 32.202];
  }

  const [lngSum, latSum] = source.reduce(
    (acc, item) => [acc[0] + item.longitude, acc[1] + item.latitude],
    [0, 0],
  );
  return [lngSum / source.length, latSum / source.length];
}

function initMap(points, vehicles) {
  const center = computeCenter(points, vehicles);
  map = new AMap.Map("map", {
    zoom: 15.2,
    center,
    viewMode: "2D",
    mapStyle: "amap://styles/grey",
  });
}

function clearOverlays(overlays) {
  overlays.forEach((overlay) => {
    overlay.setMap(null);
  });
  overlays.length = 0;
}

function renderPoints(points) {
  if (!map) {
    return;
  }
  clearOverlays(pointOverlays);
  points.forEach((point) => {
    const circle = new AMap.Circle({
      center: [point.longitude, point.latitude],
      radius: point.radius_m,
      strokeColor: "rgba(136,255,207,0.28)",
      strokeWeight: 1,
      fillColor: "rgba(136,255,207,0.025)",
      fillOpacity: 0.38,
    });
    circle.setMap(map);
    pointOverlays.push(circle);
  });
  updateCoverageVisibility();
}

function updateCoverageVisibility() {
  if (!map) {
    return;
  }
  const visible = document.getElementById("coverage-toggle").checked;
  pointOverlays.forEach((overlay) => {
    overlay.setMap(visible ? map : null);
  });
}

function buildVehicleTooltip(vehicle) {
  const pointName = pointNameById.get(vehicle.point_id) ?? vehicle.point_id;
  const badgeText = vehicle.bucket === "danche" ? "单车" : "助力";
  return `
    <div class="vehicle-tooltip">
      <div class="vehicle-tooltip__head">
        <div class="vehicle-tooltip__title">${escapeHtml(safeText(vehicle.number))}</div>
        <div class="vehicle-tooltip__badge vehicle-tooltip__badge--${vehicle.bucket}">${badgeText}</div>
      </div>
      <div class="vehicle-tooltip__grid">
        <span>锁号</span>
        <strong>${escapeHtml(safeText(vehicle.vendor_lock_id))}</strong>
        <span>电池</span>
        <strong>${escapeHtml(safeText(vehicle.battery_name))}</strong>
        <span>距离</span>
        <strong>${vehicle.distance_m == null ? "-" : `${vehicle.distance_m.toFixed(1)}m`}</strong>
        <span>采样点</span>
        <strong>${escapeHtml(pointName)}</strong>
        <span>时间</span>
        <strong>${escapeHtml(formatTime(vehicle.observed_at))}</strong>
        <span>坐标</span>
        <strong>${vehicle.longitude.toFixed(6)}, ${vehicle.latitude.toFixed(6)}</strong>
      </div>
    </div>
  `;
}

function buildVehicleMarker(vehicle) {
  const markerNode = document.createElement("div");
  markerNode.className = `vehicle-marker vehicle-marker--${vehicle.bucket}`;
  const marker = new AMap.Marker({
    position: [vehicle.longitude, vehicle.latitude],
    content: markerNode,
    title: `${vehicle.number ?? vehicle.vehicle_uid} | ${vehicle.battery_name ?? "-"}`,
  });
  marker.on("click", () => {
    if (!vehicleInfoWindow) {
      vehicleInfoWindow = new AMap.InfoWindow({
        offset: new AMap.Pixel(0, -22),
        closeWhenClickMap: true,
      });
    }
    vehicleInfoWindow.setContent(buildVehicleTooltip(vehicle));
    vehicleInfoWindow.open(map, [vehicle.longitude, vehicle.latitude]);
  });
  return marker;
}

function renderVehicles(vehicles) {
  if (!map) {
    return;
  }
  if (vehicleInfoWindow) {
    vehicleInfoWindow.close();
  }
  clearOverlays(vehicleOverlays);
  vehicles.forEach((vehicle) => {
    const marker = buildVehicleMarker(vehicle);
    marker.setMap(map);
    vehicleOverlays.push(marker);
  });
}

function renderSummary(payload) {
  document.getElementById("namespace-pill").textContent = payload.source_namespace;
  document.getElementById("generated-at").textContent = formatTime(payload.generated_at);
  document.getElementById("latest-sweep-status").textContent = payload.latest_sweep?.status ?? "no-data";
  document.getElementById("current-vehicle-total").textContent = payload.summary.current_vehicle_total;
  document.getElementById("danche-total").textContent = payload.summary.danche_total;
  document.getElementById("zhuli-total").textContent = payload.summary.zhuli_total;
  document.getElementById("raw-observation-count").textContent =
    payload.summary.latest_sweep_raw_observation_count;
  document.getElementById("latest-unique-count").textContent =
    payload.summary.latest_sweep_unique_vehicle_count;
  document.getElementById("point-success-count").textContent =
    `${payload.summary.latest_sweep_success_count}/${payload.summary.latest_sweep_point_count}`;
  document.getElementById("point-success-foot").textContent =
    `失败 ${payload.summary.latest_sweep_failure_count} 个点位`;

  const statusChip = document.getElementById("sweep-status-chip");
  statusChip.style.borderColor =
    payload.latest_sweep?.status === "completed"
      ? "rgba(136,255,207,0.25)"
      : "rgba(255,114,98,0.25)";
}

function renderHistory(history) {
  const container = document.getElementById("history-bars");
  container.innerHTML = "";

  const maxCount = Math.max(...history.map((item) => item.unique_vehicle_count), 1);
  history.forEach((item) => {
    const row = document.createElement("div");
    row.className = "history-bar";
    row.innerHTML = `
      <span class="history-slot">${formatTime(item.logical_slot).slice(11)}</span>
      <div class="history-rail">
        <div class="history-fill" style="width:${(item.unique_vehicle_count / maxCount) * 100}%"></div>
      </div>
      <strong class="history-count">${item.unique_vehicle_count}</strong>
    `;
    container.appendChild(row);
  });
}

function renderTopPoints(topPoints) {
  const container = document.getElementById("point-list");
  container.innerHTML = "";
  topPoints.forEach((point) => {
    const row = document.createElement("div");
    row.className = "point-item";
    row.innerHTML = `
      <div>
        <strong>${point.name}</strong>
        <span>原始命中 ${point.raw_observation_count}，去重 ${point.unique_vehicle_count}</span>
      </div>
      <em>${point.unique_vehicle_count}</em>
    `;
    container.appendChild(row);
  });
}

function renderVehicleList(vehicles) {
  const container = document.getElementById("vehicle-list");
  container.innerHTML = "";

  vehicles.slice(0, 10).forEach((vehicle) => {
    const row = document.createElement("div");
    row.className = "vehicle-item";
    row.innerHTML = `
      <div class="vehicle-badge vehicle-badge--${vehicle.bucket}">
        ${vehicle.bucket === "danche" ? "单车" : "助力"}
      </div>
      <div>
        <strong>${safeText(vehicle.number)}</strong>
        <span>${safeText(vehicle.battery_name)} · ${safeText(vehicle.vendor_lock_id)}</span>
      </div>
      <div class="vehicle-meta">
        <div>${vehicle.distance_m == null ? "-" : `${vehicle.distance_m.toFixed(1)}m`}</div>
        <div>${formatTime(vehicle.observed_at).slice(11)}</div>
      </div>
    `;
    container.appendChild(row);
  });
}

async function fetchBootstrap() {
  const response = await fetch(
    `/api/dashboard/bootstrap?source_namespace=${encodeURIComponent(config.sourceNamespace)}`,
  );
  if (!response.ok) {
    throw new Error(`bootstrap failed: ${response.status}`);
  }
  return response.json();
}

async function refreshDashboard() {
  try {
    const payload = await fetchBootstrap();
    pointNameById = new Map(payload.points.map((point) => [point.id, point.name]));
    renderSummary(payload);
    renderHistory(payload.history);
    renderTopPoints(payload.top_points);
    renderVehicleList(payload.vehicles);

    if (!map) {
      await loadAmap();
      initMap(payload.points, payload.vehicles);
      renderPoints(payload.points);
    }

    renderVehicles(payload.vehicles);
  } catch (error) {
    console.error(error);
  } finally {
    refreshTimer = window.setTimeout(refreshDashboard, config.refreshIntervalSeconds * 1000);
  }
}

window.addEventListener("DOMContentLoaded", () => {
  document.getElementById("coverage-toggle").addEventListener("change", updateCoverageVisibility);
  refreshDashboard();
});
