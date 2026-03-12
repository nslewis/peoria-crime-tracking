import { get } from '../api.js';
import { getFilterParams } from '../state.js';
import { renderFilterBar } from '../components/filters.js';
import { createMetricCard, createChangeIndicator } from '../components/metric-card.js';
import { createMap, addHeatmap, addMarkers, addBoundaries, destroyMap } from '../components/map.js';
import { createBarChart, destroyChart } from '../components/charts.js';

let map = null;
let chart = null;
let fearChart = null;
let heatLayer = null;
let markerLayer = null;
let boundaryLayer = null;

const FEAR_COLORS = {
    10: '#d32f2f', 9: '#e53935', 8: '#f44336', 7: '#ef5350',
    6: '#ff5722', 5: '#ff9800', 4: '#ffc107', 3: '#ffeb3b',
    2: '#607d8b', 1: '#455a64',
};

export async function render(container) {
    container.innerHTML = `
        <div class="page-header">
            <h1 class="page-title">Crime Dashboard</h1>
            <p class="page-subtitle">Peoria, Illinois — Real-time crime intelligence</p>
        </div>
        <div id="dashboard-filters"></div>
        <div class="grid grid-4" id="dashboard-metrics"></div>
        <div style="margin-top:20px;" class="grid grid-2">
            <div class="card">
                <div class="card-header"><span class="card-title">Crime Map</span></div>
                <div class="map-container" id="dashboard-map"></div>
            </div>
            <div class="card">
                <div class="card-header">
                    <span class="card-title">Public Fear Index</span>
                    <span style="font-size:0.7rem;color:var(--text-muted);">Source: Gallup Crime Worry Survey</span>
                </div>
                <div id="fear-breakdown"></div>
            </div>
        </div>
        <div style="margin-top:20px;">
            <div class="card">
                <div class="card-header"><span class="card-title">Crime Trend</span></div>
                <div class="chart-container" style="height:300px;">
                    <canvas id="dashboard-trend-chart"></canvas>
                </div>
            </div>
        </div>
    `;

    await renderFilterBar(document.getElementById('dashboard-filters'), loadData);
    await loadData();
}

async function loadData() {
    const params = getFilterParams();

    const [severity, summary, crimes, trend] = await Promise.all([
        get('/dashboard/severity', params),
        get('/dashboard/summary', params),
        get('/dashboard/crimes', { ...params, limit: 2000 }),
        get('/dashboard/trend', params),
    ]);

    let boundaries = null;
    try {
        boundaries = await get('/meta/boundaries', { type: 'districts' });
    } catch (e) {
        console.warn('Boundaries failed to load:', e);
    }

    renderMetrics(severity, summary);
    renderFearBreakdown(summary.by_fear);
    renderMap(crimes, boundaries);
    renderTrend(trend);
}

function renderMetrics(severity, summary) {
    const metricsEl = document.getElementById('dashboard-metrics');
    metricsEl.innerHTML = '';

    metricsEl.appendChild(createMetricCard({
        value: severity.rating,
        label: 'Crime Severity',
        sub: `Score: ${severity.score.toLocaleString()}`,
        color: severity.color,
        severity: severity.rating,
    }));

    metricsEl.appendChild(createMetricCard({
        value: summary.total.toLocaleString(),
        label: 'Total Crimes',
    }));

    const yoy = summary.yoy;
    metricsEl.appendChild(createMetricCard({
        value: yoy.current ? yoy.current.toLocaleString() : '—',
        label: yoy.previous_year ? `vs ${yoy.previous_year}` : 'Year Trend',
        sub: createChangeIndicator(yoy.change_pct),
    }));

    // Top crimes
    const topDiv = document.createElement('div');
    topDiv.className = 'metric-card';
    let topHTML = '<div class="metric-label" style="margin-bottom:8px;">Top Crime Types</div>';
    if (summary.top_crimes && summary.top_crimes.length > 0) {
        for (const tc of summary.top_crimes.slice(0, 4)) {
            topHTML += `<div style="display:flex;justify-content:space-between;padding:3px 0;font-size:0.78rem;">
                <span style="color:var(--text-secondary)">${tc.type}</span>
                <span class="mono" style="color:var(--accent-danger)">${tc.count.toLocaleString()}</span>
            </div>`;
        }
    } else {
        topHTML += '<div style="color:var(--text-muted);font-size:0.82rem;">No data</div>';
    }
    topDiv.innerHTML = topHTML;
    metricsEl.appendChild(topDiv);
}

function renderFearBreakdown(byFear) {
    const el = document.getElementById('fear-breakdown');
    if (!byFear || byFear.length === 0) {
        el.innerHTML = '<p style="color:var(--text-muted)">No data</p>';
        return;
    }

    let html = '<div style="display:flex;flex-direction:column;gap:6px;max-height:460px;overflow-y:auto;padding-right:8px;">';
    for (const item of byFear) {
        const fear = item.fear_level;
        const color = FEAR_COLORS[fear] || '#607d8b';
        const maxCount = byFear[0].count || 1;
        // Use log scale for bar width so low-count high-fear crimes are still visible
        const barPct = Math.max(8, (Math.log(item.count + 1) / Math.log(maxCount + 1)) * 100);

        let fearLabel, fearBadge;
        if (fear >= 8) { fearLabel = 'HIGH FEAR'; fearBadge = 'badge-danger'; }
        else if (fear >= 5) { fearLabel = 'MODERATE'; fearBadge = 'badge-warning'; }
        else { fearLabel = 'LOW'; fearBadge = 'badge-info'; }

        html += `
        <div style="padding:8px 12px;background:var(--bg-input);border-radius:6px;border-left:3px solid ${color};">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
                <span style="font-size:0.8rem;font-weight:500;color:var(--text-primary);">${item.type}</span>
                <span class="badge ${fearBadge}" style="font-size:0.65rem;">${fearLabel} ${fear}/10</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
                <div style="flex:1;height:6px;background:rgba(42,42,74,0.5);border-radius:3px;overflow:hidden;">
                    <div style="width:${barPct}%;height:100%;background:${color};border-radius:3px;"></div>
                </div>
                <span class="mono" style="font-size:0.75rem;color:${color};min-width:50px;text-align:right;">${item.count.toLocaleString()}</span>
            </div>
        </div>`;
    }
    html += '</div>';
    el.innerHTML = html;
}

function renderMap(crimes, boundaries) {
    if (heatLayer) { map.removeLayer(heatLayer); heatLayer = null; }
    if (markerLayer) { map.removeLayer(markerLayer); markerLayer = null; }
    if (boundaryLayer) { map.removeLayer(boundaryLayer); boundaryLayer = null; }

    if (!map) {
        map = createMap('dashboard-map');
    }

    heatLayer = addHeatmap(map, crimes);
    markerLayer = addMarkers(map, crimes, 300);
    boundaryLayer = addBoundaries(map, boundaries);
}

function renderTrend(trend) {
    if (chart) destroyChart(chart);

    const canvas = document.getElementById('dashboard-trend-chart');
    if (!trend || trend.length === 0) return;

    const labels = trend.map(t => `${t.year}-${String(t.month).padStart(2, '0')}`);
    const data = trend.map(t => t.count);
    chart = createBarChart(canvas, labels, data, { label: 'Crimes' });
}

export function destroy() {
    if (chart) { destroyChart(chart); chart = null; }
    if (fearChart) { destroyChart(fearChart); fearChart = null; }
    if (map) { destroyMap(map); map = null; }
    heatLayer = null;
    markerLayer = null;
    boundaryLayer = null;
}
