import { get } from '../api.js';
import { createMap, addMarkers, destroyMap, crimeColor } from '../components/map.js';
import { createBarChart, destroyChart } from '../components/charts.js';
import { renderDataTable } from '../components/data-table.js';

let debounceTimer = null;
let map = null;
let chart = null;
let markerLayer = null;

export async function render(container) {
    container.innerHTML = `
        <div class="page-header">
            <h1 class="page-title">Street Search</h1>
            <p class="page-subtitle">Search for crime activity on any street in Peoria</p>
        </div>
        <div class="search-box">
            <span class="search-icon">&#128269;</span>
            <input type="search" id="street-input" placeholder="Search a street name (e.g. Main, War Memorial)..." autocomplete="off">
        </div>
        <div id="street-results"></div>
        <div id="street-detail" style="display:none;">
            <button class="btn btn-sm" id="street-back" style="margin-bottom:16px;">&larr; Back to results</button>
            <div id="street-detail-content"></div>
        </div>
    `;

    const input = document.getElementById('street-input');
    input.addEventListener('input', () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => searchStreets(input.value), 350);
    });

    document.getElementById('street-back').addEventListener('click', () => {
        document.getElementById('street-detail').style.display = 'none';
        document.getElementById('street-results').style.display = 'block';
    });
}

async function searchStreets(query) {
    const resultsEl = document.getElementById('street-results');
    if (query.length < 2) {
        resultsEl.innerHTML = '';
        return;
    }
    resultsEl.innerHTML = '<div class="loading-screen" style="min-height:100px"><div class="loading-inline"></div></div>';

    try {
        const results = await get('/streets/search', { q: query });
        if (results.length === 0) {
            resultsEl.innerHTML = '<p style="color:var(--text-secondary);padding:20px;">No streets found.</p>';
            return;
        }

        resultsEl.innerHTML = '';
        const columns = [
            { key: 'address', label: 'Address' },
            { key: 'crime_count', label: 'Crimes', mono: true },
            { key: 'crime_types', label: 'Types', render: v => v ? v.split(',').slice(0, 3).join(', ') : '' },
            { key: 'latest', label: 'Latest', mono: true, render: v => v ? v.slice(0, 10) : '' },
        ];
        renderClickableTable(resultsEl, columns, results);
    } catch (err) {
        resultsEl.innerHTML = `<div class="error-message">${err.message}</div>`;
    }
}

function renderClickableTable(container, columns, rows) {
    const wrap = document.createElement('div');
    wrap.className = 'data-table-wrap';
    const table = document.createElement('table');
    table.className = 'data-table';

    const thead = document.createElement('thead');
    const hr = document.createElement('tr');
    for (const col of columns) {
        const th = document.createElement('th');
        th.textContent = col.label;
        hr.appendChild(th);
    }
    thead.appendChild(hr);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const row of rows) {
        const tr = document.createElement('tr');
        tr.style.cursor = 'pointer';
        for (const col of columns) {
            const td = document.createElement('td');
            const val = row[col.key];
            if (col.render) {
                td.innerHTML = col.render(val, row);
            } else {
                td.textContent = val ?? '';
            }
            if (col.mono) td.classList.add('mono');
            tr.appendChild(td);
        }
        tr.addEventListener('click', () => loadDetail(row.address));
        tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    wrap.appendChild(table);
    container.appendChild(wrap);
}

async function loadDetail(address) {
    document.getElementById('street-results').style.display = 'none';
    const detailEl = document.getElementById('street-detail');
    detailEl.style.display = 'block';
    const content = document.getElementById('street-detail-content');
    content.innerHTML = '<div class="loading-screen" style="min-height:200px"><div class="loading-inline"></div></div>';

    try {
        const data = await get('/streets/detail', { address });
        content.innerHTML = `
            <h2 style="font-size:1.2rem;margin-bottom:16px;">${address}</h2>
            <div class="grid grid-3" id="street-metrics"></div>
            <div style="margin-top:20px;" class="grid grid-2">
                <div class="card">
                    <div class="card-header"><span class="card-title">Crime Map</span></div>
                    <div class="map-container" id="street-map" style="height:350px;"></div>
                </div>
                <div class="card">
                    <div class="card-header"><span class="card-title">By Year</span></div>
                    <div class="chart-container" style="height:300px;">
                        <canvas id="street-year-chart"></canvas>
                    </div>
                </div>
            </div>
            <div style="margin-top:20px;">
                <div class="card">
                    <div class="card-header"><span class="card-title">Recent Crimes</span></div>
                    <div id="street-recent-table"></div>
                </div>
            </div>
        `;

        // Metrics
        const metrics = document.getElementById('street-metrics');
        metrics.innerHTML = `
            <div class="metric-card">
                <div class="metric-value" style="color:var(--accent-danger)">${data.total.toLocaleString()}</div>
                <div class="metric-label">Total Crimes</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">${data.severity_score.toLocaleString()}</div>
                <div class="metric-label">Severity Score</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">${data.by_type.length}</div>
                <div class="metric-label">Crime Categories</div>
            </div>
        `;

        // Map
        if (map) destroyMap(map);
        map = createMap('street-map', { zoom: 14 });
        if (data.recent.length > 0) {
            markerLayer = addMarkers(map, data.recent, 100);
            const first = data.recent.find(c => c.latitude && c.longitude);
            if (first) map.setView([first.latitude, first.longitude], 15);
        }

        // Year chart
        if (chart) destroyChart(chart);
        if (data.by_year.length > 0) {
            chart = createBarChart(
                document.getElementById('street-year-chart'),
                data.by_year.map(y => String(y.year)),
                data.by_year.map(y => y.count),
            );
        }

        // Recent table
        const cols = [
            { key: 'nibrs_offense', label: 'Offense' },
            { key: 'nibrs_description', label: 'Description' },
            { key: 'report_date', label: 'Date', mono: true, render: v => v ? v.slice(0, 10) : '' },
            { key: 'address', label: 'Address' },
        ];
        renderDataTable(document.getElementById('street-recent-table'), cols, data.recent, { filename: `street-${address}` });
    } catch (err) {
        content.innerHTML = `<div class="error-message">${err.message}</div>`;
    }
}

export function destroy() {
    clearTimeout(debounceTimer);
    if (chart) { destroyChart(chart); chart = null; }
    if (map) { destroyMap(map); map = null; }
    markerLayer = null;
}
