import { get, post } from '../api.js';
import { showToast } from '../components/toast.js';

export async function render(container) {
    container.innerHTML = `
        <div class="page-header">
            <h1 class="page-title">Data Sync</h1>
            <p class="page-subtitle">Manage data sources and synchronization</p>
        </div>
        <div class="grid grid-2">
            <div class="card">
                <div class="card-header">
                    <span class="card-title">Record Counts</span>
                    <button class="btn btn-sm" id="refresh-counts">Refresh</button>
                </div>
                <div id="sync-counts"></div>
            </div>
            <div class="card">
                <div class="card-header"><span class="card-title">Sync Actions</span></div>
                <div style="display:flex;flex-direction:column;gap:12px;">
                    <button class="btn btn-primary" id="sync-full">Run Full Sync</button>
                    <div class="grid grid-2" style="gap:8px;">
                        <button class="btn btn-sm" data-source="crimes">Sync Crimes</button>
                        <button class="btn btn-sm" data-source="calls_for_service">Sync Calls</button>
                        <button class="btn btn-sm" data-source="shotspotter">Sync ShotSpotter</button>
                        <button class="btn btn-sm" data-source="boundaries">Sync Boundaries</button>
                    </div>
                    <p style="font-size:0.75rem;color:var(--text-muted);">
                        Data source: Peoria PD ArcGIS (public, no auth required).
                        Full sync may take several minutes.
                    </p>
                </div>
            </div>
        </div>
        <div style="margin-top:20px;">
            <div class="card">
                <div class="card-header"><span class="card-title">Sync History</span></div>
                <div id="sync-history"></div>
            </div>
        </div>
    `;

    // Load data
    await Promise.all([loadCounts(), loadHistory()]);

    // Event handlers
    document.getElementById('refresh-counts').addEventListener('click', loadCounts);

    document.getElementById('sync-full').addEventListener('click', async () => {
        const btn = document.getElementById('sync-full');
        btn.disabled = true;
        btn.textContent = 'Syncing...';
        try {
            await post('/sync/full');
            showToast('Full sync started — this may take a few minutes', 'success');
        } catch (err) {
            showToast('Sync failed: ' + err.message, 'error');
        }
        btn.disabled = false;
        btn.textContent = 'Run Full Sync';
    });

    // Individual source syncs
    container.querySelectorAll('[data-source]').forEach(btn => {
        btn.addEventListener('click', async () => {
            const source = btn.dataset.source;
            btn.disabled = true;
            const orig = btn.textContent;
            btn.textContent = 'Syncing...';
            try {
                await post('/sync/source', { source });
                showToast(`${source} sync started`, 'success');
            } catch (err) {
                showToast('Sync failed: ' + err.message, 'error');
            }
            btn.disabled = false;
            btn.textContent = orig;
        });
    });
}

async function loadCounts() {
    const el = document.getElementById('sync-counts');
    try {
        const counts = await get('/meta/table-counts');
        el.innerHTML = `
            <div style="display:flex;flex-direction:column;gap:12px;">
                ${Object.entries(counts).map(([name, count]) => `
                    <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);">
                        <span style="font-size:0.85rem;">${formatSourceName(name)}</span>
                        <span class="badge badge-info" style="font-size:0.82rem;">${count.toLocaleString()}</span>
                    </div>
                `).join('')}
            </div>
        `;
    } catch (err) {
        el.innerHTML = `<div class="error-message">${err.message}</div>`;
    }
}

async function loadHistory() {
    const el = document.getElementById('sync-history');
    try {
        const history = await get('/sync/history');
        if (history.length === 0) {
            el.innerHTML = '<p style="color:var(--text-muted);padding:12px;">No sync history yet.</p>';
            return;
        }
        let html = '<div class="data-table-wrap"><table class="data-table"><thead><tr>' +
            '<th>Source</th><th>Table</th><th>Records</th><th>Started</th><th>Status</th>' +
            '</tr></thead><tbody>';
        for (const h of history) {
            const statusClass = h.status === 'completed' ? 'badge-success' : 'badge-warning';
            html += `<tr>
                <td>${h.source || ''}</td>
                <td>${h.table_name || ''}</td>
                <td class="mono">${(h.records_fetched || 0).toLocaleString()}</td>
                <td class="mono">${(h.started_at || '').slice(0, 19)}</td>
                <td><span class="badge ${statusClass}">${h.status || ''}</span></td>
            </tr>`;
        }
        html += '</tbody></table></div>';
        el.innerHTML = html;
    } catch (err) {
        el.innerHTML = `<div class="error-message">${err.message}</div>`;
    }
}

function formatSourceName(name) {
    return name.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

export function destroy() {}
