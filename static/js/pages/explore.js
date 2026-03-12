import { get } from '../api.js';
import { renderDataTable } from '../components/data-table.js';
import { createMap, addMarkers, destroyMap } from '../components/map.js';

let map = null;
let markerLayer = null;
let currentSource = 'crimes';

const SOURCE_COLUMNS = {
    crimes: [
        { key: 'nibrs_offense', label: 'Offense' },
        { key: 'nibrs_description', label: 'Description' },
        { key: 'address', label: 'Address' },
        { key: 'district', label: 'District', mono: true },
        { key: 'report_date', label: 'Date', mono: true, render: v => v ? v.slice(0, 10) : '' },
    ],
    calls_for_service: [
        { key: 'call_type', label: 'Type' },
        { key: 'priority', label: 'Priority', mono: true },
        { key: 'disposition', label: 'Disposition' },
        { key: 'address', label: 'Address' },
        { key: 'call_date', label: 'Date', mono: true, render: v => v ? v.slice(0, 10) : '' },
    ],
    shotspotter: [
        { key: 'event_type', label: 'Event' },
        { key: 'rounds_fired', label: 'Rounds', mono: true },
        { key: 'address', label: 'Address' },
        { key: 'district', label: 'District', mono: true },
        { key: 'event_date', label: 'Date', mono: true, render: v => v ? v.slice(0, 10) : '' },
    ],
};

export async function render(container) {
    container.innerHTML = `
        <div class="page-header">
            <h1 class="page-title">Explore Data</h1>
            <p class="page-subtitle">Browse crime records, calls for service, and ShotSpotter data</p>
        </div>
        <div class="tabs" id="explore-tabs">
            <button class="tab active" data-source="crimes">Crimes</button>
            <button class="tab" data-source="calls_for_service">Calls for Service</button>
            <button class="tab" data-source="shotspotter">ShotSpotter</button>
        </div>
        <div class="grid grid-1">
            <div class="card">
                <div class="card-header">
                    <span class="card-title" id="explore-count">Loading...</span>
                    <div style="display:flex;gap:8px;">
                        <select id="explore-district" style="width:auto;">
                            <option value="">All Districts</option>
                        </select>
                    </div>
                </div>
                <div id="explore-table"></div>
            </div>
            <div class="card">
                <div class="card-header"><span class="card-title">Location Map</span></div>
                <div class="map-container" id="explore-map" style="height:400px;"></div>
            </div>
        </div>
    `;

    // Load district options
    const opts = await get('/meta/options');
    const distSel = document.getElementById('explore-district');
    for (const d of opts.districts) {
        const o = document.createElement('option');
        o.value = d.id;
        o.textContent = d.name;
        distSel.appendChild(o);
    }
    distSel.addEventListener('change', loadData);

    // Tab clicks
    document.getElementById('explore-tabs').addEventListener('click', (e) => {
        if (!e.target.classList.contains('tab')) return;
        document.querySelectorAll('#explore-tabs .tab').forEach(t => t.classList.remove('active'));
        e.target.classList.add('active');
        currentSource = e.target.dataset.source;
        loadData();
    });

    await loadData();
}

async function loadData() {
    const district = document.getElementById('explore-district').value;
    const tableEl = document.getElementById('explore-table');
    const countEl = document.getElementById('explore-count');

    tableEl.innerHTML = '<div class="loading-screen" style="min-height:100px"><div class="loading-inline"></div></div>';

    try {
        const data = await get(`/explore/${currentSource}`, { district, limit: 200 });
        countEl.textContent = `${data.total.toLocaleString()} records`;

        tableEl.innerHTML = '';
        const columns = SOURCE_COLUMNS[currentSource];
        renderDataTable(tableEl, columns, data.records, { filename: currentSource });

        // Map
        if (markerLayer && map) { map.removeLayer(markerLayer); markerLayer = null; }
        if (!map) {
            map = createMap('explore-map');
        }
        const latKey = currentSource === 'crimes' ? 'latitude' : 'latitude';
        markerLayer = addMarkers(map, data.records, 300);
    } catch (err) {
        tableEl.innerHTML = `<div class="error-message">${err.message}</div>`;
    }
}

export function destroy() {
    if (map) { destroyMap(map); map = null; }
    markerLayer = null;
}
