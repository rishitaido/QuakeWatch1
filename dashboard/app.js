/**
 * QuakeWatch - Geo Dashboard Application
 * Manages the Leaflet map, earthquake markers, alert banner,
 * alert log, stats, and filter controls.
 * Owner: Haina
 */

// ── Configuration ──────────────────────────────────────────
const API_BASE = "/api"; // Proxied through nginx to FastAPI
const POLL_INTERVAL_MS = 10000; // Refresh every 10 seconds
const STALE_THRESHOLD_MS = 120000; // Show warning after 2 min without data

// ── State ──────────────────────────────────────────────────
let map;
let markersLayer;
let lastUpdateTime = Date.now();
let lastBannerEventId = null;
let bannerDismissed = false;
let knownEventIds = new Set();
let alertLogCollapsed = false;

// ── Severity Colors ────────────────────────────────────────
const SEVERITY_COLORS = {
    high: "#e74c3c",
    medium: "#f39c12",
    low: "#7f8c8d",
};

const SEVERITY_FILL = {
    high: "rgba(231, 76, 60, 0.5)",
    medium: "rgba(243, 156, 18, 0.4)",
    low: "rgba(127, 140, 141, 0.3)",
};

// ══════════════════════════════════════════════════════════
// MAP INITIALIZATION
// ══════════════════════════════════════════════════════════

function initMap() {
    map = L.map("map", {
        center: [20, 0],
        zoom: 2,
        minZoom: 2,
        maxZoom: 18,
        zoomControl: true,
        attributionControl: true,
    });

    // Dark-themed tile layer (CartoDB Dark Matter)
    L.tileLayer(
        "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        {
            attribution:
                '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
            subdomains: "abcd",
            maxZoom: 19,
        }
    ).addTo(map);

    // Layer group for earthquake markers
    markersLayer = L.layerGroup().addTo(map);

    // Re-measure container after flex layout settles so tiles and markers stay aligned
    setTimeout(() => map.invalidateSize(), 0);
}

// ══════════════════════════════════════════════════════════
// DATA FETCHING
// ══════════════════════════════════════════════════════════

/**
 * Fetch earthquake data from the REST API with current filter values.
 */
async function fetchEarthquakes() {
    const hours = document.getElementById("filter-time").value;
    const minMag = document.getElementById("filter-mag").value;
    const minImpact = document.getElementById("filter-impact").value;

    const url = `${API_BASE}/earthquakes?hours=${hours}&min_mag=${minMag}&min_impact=${minImpact}`;

    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const data = await response.json();
        setConnectionStatus("connected");
        lastUpdateTime = Date.now();

        return data.earthquakes || [];
    } catch (error) {
        console.error("Failed to fetch earthquakes:", error);
        setConnectionStatus("disconnected");
        return null; // null = error, [] = empty but successful
    }
}

/**
 * Fetch alerts from the REST API.
 */
async function fetchAlerts() {
    try {
        const response = await fetch(`${API_BASE}/alerts?hours=24`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const data = await response.json();
        return data.alerts || [];
    } catch (error) {
        console.error("Failed to fetch alerts:", error);
        return [];
    }
}

/**
 * Fetch summary statistics from the REST API.
 */
async function fetchStats() {
    try {
        const response = await fetch(`${API_BASE}/stats`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        return await response.json();
    } catch (error) {
        console.error("Failed to fetch stats:", error);
        return null;
    }
}

// ══════════════════════════════════════════════════════════
// MAP MARKERS
// ══════════════════════════════════════════════════════════

/**
 * Calculate marker radius based on earthquake magnitude.
 * Bigger earthquakes get bigger circles.
 */
function getMarkerRadius(magnitude) {
    if (magnitude <= 0) return 3;
    return Math.max(4, magnitude * 4);
}

/**
 * Parse USGS place labels like "166 km W of Abepura, Indonesia".
 */
function parsePlaceReference(place) {
    if (!place || typeof place !== "string") return null;

    const normalized = place.trim().replace(/\s+/g, " ");
    const match = normalized.match(
        /^(\d+(?:\.\d+)?)\s*km\s+[A-Za-z]{1,3}\s+of\s+(.+)$/i
    );
    if (!match) return null;

    const locality = match[2].trim();
    const cityName = locality.split(",")[0].trim() || locality;

    return {
        name: cityName,
        distanceKm: Number(match[1]),
    };
}

/**
 * Choose a user-facing nearest city label.
 * Uses the feed's reference locality when it is clearly closer.
 */
function getDisplayNearestInfo(item) {
    const placeRef = parsePlaceReference(item.place || "");
    const fallbackName = item.nearest_city || "Unknown";
    const fallbackDist = Number(item.nearest_city_dist_km);
    const hasFallbackDist = Number.isFinite(fallbackDist) && fallbackDist >= 0;

    if (
        placeRef &&
        (fallbackName === "Unknown" ||
            !hasFallbackDist ||
            placeRef.distanceKm + 25 < fallbackDist)
    ) {
        return placeRef;
    }

    return {
        name: fallbackName,
        distanceKm: hasFallbackDist ? fallbackDist : null,
    };
}

/**
 * Build HTML popup content for an earthquake marker.
 */
function buildPopupContent(quake) {
    const severity = String(quake.severity || "low").toLowerCase();
    const timeAgo = getTimeAgo(quake.timestamp);
    const mag = Number(quake.magnitude || 0).toFixed(1);
    const depth = Number(quake.depth ?? quake.depth_km ?? 0).toFixed(1);
    const impact = Number(quake.impact_score || 0).toFixed(1);
    const nearestInfo = getDisplayNearestInfo(quake);
    const nearestCityLabel =
        nearestInfo.distanceKm == null
            ? nearestInfo.name
            : `${nearestInfo.name} (${nearestInfo.distanceKm.toFixed(0)} km)`;

    return `
        <div class="popup-title">${quake.place || "Unknown Location"}</div>
        <div class="popup-row">
            <span class="popup-label">Magnitude</span>
            <span class="popup-value ${severity}">${mag}</span>
        </div>
        <div class="popup-row">
            <span class="popup-label">Depth</span>
            <span class="popup-value">${depth} km</span>
        </div>
        <div class="popup-row">
            <span class="popup-label">Impact Score</span>
            <span class="popup-value ${severity}">${impact}/100</span>
        </div>
        <div class="popup-row">
            <span class="popup-label">Nearest City</span>
            <span class="popup-value">${nearestCityLabel}</span>
        </div>
        <div class="popup-row">
            <span class="popup-label">Severity</span>
            <span class="popup-value ${severity}">${severity.toUpperCase()}</span>
        </div>
        <div class="popup-row">
            <span class="popup-label">Time</span>
            <span class="popup-value">${timeAgo}</span>
        </div>
    `;
}

/**
 * Update all earthquake markers on the map.
 * New earthquakes get a pulse animation.
 */
function updateMarkers(earthquakes) {
    markersLayer.clearLayers();

    const newEventIds = new Set();

    earthquakes.forEach((quake) => {
        if (quake.lat == null || quake.lon == null) return;

        const severity = String(quake.severity || "low").toLowerCase();
        const isNew = !knownEventIds.has(quake.event_id);
        newEventIds.add(quake.event_id);

        const marker = L.circleMarker([quake.lat, quake.lon], {
            radius: getMarkerRadius(quake.magnitude),
            fillColor: SEVERITY_FILL[severity] || SEVERITY_FILL.low,
            color: SEVERITY_COLORS[severity] || SEVERITY_COLORS.low,
            weight: 2,
            opacity: 0.9,
            fillOpacity: 0.6,
            className: isNew ? "quake-pulse" : "",
        });

        marker.bindPopup(buildPopupContent(quake), {
            maxWidth: 280,
            className: "quake-popup",
        });

        markersLayer.addLayer(marker);
    });

    knownEventIds = newEventIds;
}

// ══════════════════════════════════════════════════════════
// ALERT BANNER
// ══════════════════════════════════════════════════════════

/**
 * Keep the map/sidebar layout below the fixed alert banner.
 */
function syncBannerOffset() {
    const banner = document.getElementById("alert-banner");
    const isVisible = !banner.classList.contains("hidden");

    if (!isVisible) {
        document.documentElement.style.setProperty("--banner-offset", "0px");
        if (map) setTimeout(() => map.invalidateSize(), 0);
        return;
    }

    const height = Math.ceil(banner.getBoundingClientRect().height);
    document.documentElement.style.setProperty("--banner-offset", `${height}px`);
    if (map) setTimeout(() => map.invalidateSize(), 0);
}

/**
 * Show or update the alert banner based on the most recent high/medium alert.
 */
function updateAlertBanner(alerts) {
    const banner = document.getElementById("alert-banner");
    const bannerText = document.getElementById("alert-banner-text");

    if (!alerts || alerts.length === 0) {
        if (!bannerDismissed) {
            banner.classList.add("hidden");
            banner.classList.remove("high", "medium");
            syncBannerOffset();
        }
        return;
    }

    // Show the most recent alert
    const latest = alerts[0];

    // Don't re-show if user dismissed this specific alert
    if (bannerDismissed && latest.event_id === lastBannerEventId) {
        return;
    }

    // New alert -- show it
    if (latest.event_id !== lastBannerEventId) {
        bannerDismissed = false;
        lastBannerEventId = latest.event_id;
    }

    const severity = (latest.severity || "medium").toLowerCase();
    const mag = parseFloat(latest.magnitude || 0).toFixed(1);
    const impact = parseFloat(latest.impact_score || 0).toFixed(0);
    const nearestInfo = getDisplayNearestInfo(latest);

    banner.classList.remove("hidden", "high", "medium");
    banner.classList.add(severity);
    bannerText.textContent = `${severity.toUpperCase()} ALERT: M${mag} earthquake near ${latest.place || "Unknown"} | Impact Score: ${impact}/100 | ${nearestInfo.name || "Unknown"}`;
    syncBannerOffset();
}

/**
 * Dismiss the current alert banner.
 */
function dismissBanner() {
    const banner = document.getElementById("alert-banner");
    banner.classList.add("hidden");
    bannerDismissed = true;
    syncBannerOffset();
}

// Make dismissBanner available globally for the onclick handler
window.dismissBanner = dismissBanner;

// ══════════════════════════════════════════════════════════
// ALERT LOG
// ══════════════════════════════════════════════════════════

function setAlertLogCollapsed(collapsed) {
    const section = document.getElementById("alert-log-section");
    const toggle = document.getElementById("alert-log-toggle");
    if (!section || !toggle) return;

    alertLogCollapsed = collapsed;
    section.classList.toggle("collapsed", collapsed);
    toggle.textContent = collapsed ? "Open Alerts" : "Collapse";
    toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
    if (map) {
        setTimeout(() => map.invalidateSize(), 0);
    }
}

function ensureAlertLogToggleControl() {
    const section = document.getElementById("alert-log-section");
    if (!section) return null;

    let toggle = document.getElementById("alert-log-toggle");
    if (toggle) return toggle;

    const log = document.getElementById("alert-log");
    let title = section.querySelector(".section-title");
    let row = section.querySelector(".section-title-row");

    if (!title) {
        title = document.createElement("h3");
        title.className = "section-title";
        title.textContent = "Alert Log";
    }

    if (!row) {
        row = document.createElement("div");
        row.className = "section-title-row";
        section.insertBefore(row, log || section.firstChild);
    }

    if (title.parentElement !== row) {
        row.prepend(title);
    }

    toggle = document.createElement("button");
    toggle.id = "alert-log-toggle";
    toggle.className = "section-toggle-btn";
    toggle.type = "button";
    toggle.setAttribute("aria-expanded", "true");
    toggle.setAttribute("aria-controls", "alert-log");
    toggle.textContent = "Collapse";
    row.appendChild(toggle);

    return toggle;
}

function initAlertLogToggle() {
    const toggle = ensureAlertLogToggleControl();
    if (!toggle) return;

    const saved = window.localStorage.getItem("quakewatch.alert_log_collapsed");
    setAlertLogCollapsed(saved === "true");

    toggle.addEventListener("click", () => {
        setAlertLogCollapsed(!alertLogCollapsed);
        window.localStorage.setItem(
            "quakewatch.alert_log_collapsed",
            String(alertLogCollapsed)
        );
    });
}

/**
 * Update the right-side alert log with recent alerts.
 */
function updateAlertLog(alerts) {
    const log = document.getElementById("alert-log");

    if (!alerts || alerts.length === 0) {
        log.innerHTML = '<div class="alert-log-empty">No alerts in the last 24 hours</div>';
        return;
    }

    log.innerHTML = alerts
        .slice(0, 20)
        .map((alert) => {
            const severity = (alert.severity || "medium").toLowerCase();
            const mag = parseFloat(alert.magnitude || 0).toFixed(1);
            const impact = parseFloat(alert.impact_score || 0).toFixed(0);
            const timeAgo = getTimeAgo(alert.timestamp || alert.created_at);
            const nearestInfo = getDisplayNearestInfo(alert);
            const nearestLabel =
                nearestInfo.distanceKm == null
                    ? nearestInfo.name
                    : `${nearestInfo.name} (${nearestInfo.distanceKm.toFixed(0)} km)`;

            return `
                <div class="alert-log-item ${severity}">
                    <div class="alert-place">
                        <span class="severity-badge ${severity}">${severity}</span>
                        M${mag} - ${alert.place || "Unknown"}
                    </div>
                    <div class="alert-details">
                        Impact: ${impact}/100 | Near: ${nearestLabel}
                    </div>
                    <div class="alert-time">${timeAgo}</div>
                </div>
            `;
        })
        .join("");
}

// ══════════════════════════════════════════════════════════
// STATS
// ══════════════════════════════════════════════════════════

/**
 * Update the stats cards in the sidebar.
 */
function updateStats(stats) {
    if (!stats) return;

    document.getElementById("stat-total").textContent =
        stats.total_events_24h ?? 0;
    document.getElementById("stat-max-mag").textContent =
        stats.highest_magnitude != null ? parseFloat(stats.highest_magnitude).toFixed(1) : "--";
    document.getElementById("stat-max-impact").textContent =
        stats.highest_impact != null ? parseFloat(stats.highest_impact).toFixed(0) : "--";
    document.getElementById("stat-alerts").textContent =
        stats.total_alerts_24h ?? 0;
}

// ══════════════════════════════════════════════════════════
// CONNECTION STATUS
// ══════════════════════════════════════════════════════════

/**
 * Update the connection status indicator.
 * @param {"connected"|"disconnected"|"stale"} status
 */
function setConnectionStatus(status) {
    const el = document.getElementById("connection-status");
    const text = document.getElementById("connection-text");

    el.classList.remove("connected", "disconnected", "stale");
    el.classList.add(status);

    const labels = {
        connected: "Connected",
        disconnected: "Connection Lost",
        stale: "Stale Data",
    };
    text.textContent = labels[status] || "Unknown";
}

/**
 * Check if data is stale (no update in 2+ minutes).
 */
function checkStaleData() {
    const elapsed = Date.now() - lastUpdateTime;
    if (elapsed > STALE_THRESHOLD_MS) {
        setConnectionStatus("stale");
    }
}

// ══════════════════════════════════════════════════════════
// FILTERS
// ══════════════════════════════════════════════════════════

/**
 * Set up filter event listeners. Trigger a refresh when filters change.
 */
function initFilters() {
    document.getElementById("filter-time").addEventListener("change", refreshData);

    const magSlider = document.getElementById("filter-mag");
    magSlider.addEventListener("input", () => {
        document.getElementById("mag-display").textContent = magSlider.value;
    });
    magSlider.addEventListener("change", refreshData);

    const impactSlider = document.getElementById("filter-impact");
    impactSlider.addEventListener("input", () => {
        document.getElementById("impact-display").textContent = impactSlider.value;
    });
    impactSlider.addEventListener("change", refreshData);
}

// ══════════════════════════════════════════════════════════
// UTILITIES
// ══════════════════════════════════════════════════════════

/**
 * Convert a timestamp (milliseconds) to a human-readable "time ago" string.
 */
function getTimeAgo(timestampMs) {
    if (!timestampMs) return "Unknown";

    const now = Date.now();
    const diffMs = now - timestampMs;
    const diffMin = Math.floor(diffMs / 60000);
    const diffHr = Math.floor(diffMs / 3600000);

    if (diffMin < 1) return "Just now";
    if (diffMin < 60) return `${diffMin}m ago`;
    if (diffHr < 24) return `${diffHr}h ago`;
    return `${Math.floor(diffHr / 24)}d ago`;
}

/**
 * Update the "last updated" footer timestamp.
 */
function updateTimestamp() {
    const el = document.getElementById("last-updated");
    el.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
}

// ══════════════════════════════════════════════════════════
// MAIN REFRESH LOOP
// ══════════════════════════════════════════════════════════

/**
 * Fetch all data and update the UI. Called on interval and when filters change.
 */
async function refreshData() {
    // Fetch all data in parallel
    const [earthquakes, alerts, stats] = await Promise.all([
        fetchEarthquakes(),
        fetchAlerts(),
        fetchStats(),
    ]);

    // Update UI components
    if (earthquakes !== null) {
        updateMarkers(earthquakes);
    }
    updateAlertBanner(alerts);
    updateAlertLog(alerts);
    updateStats(stats);
    updateTimestamp();
}

// ══════════════════════════════════════════════════════════
// INITIALIZATION
// ══════════════════════════════════════════════════════════

document.addEventListener("DOMContentLoaded", () => {
    console.log("QuakeWatch Dashboard initializing...");

    // Initialize map
    initMap();

    // Initialize filter listeners
    initFilters();
    initAlertLogToggle();

    // First data load
    refreshData();
    syncBannerOffset();

    // Start polling loop
    setInterval(refreshData, POLL_INTERVAL_MS);

    // Check for stale data every 30 seconds
    setInterval(checkStaleData, 30000);
    window.addEventListener("resize", syncBannerOffset);

    console.log("QuakeWatch Dashboard ready!");
});
