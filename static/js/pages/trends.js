import { get } from '../api.js';
import { loadOptions } from '../state.js';
import { createLineChart, createBarChart, createHeatmapMatrix, destroyChart } from '../components/charts.js';

let charts = [];

export async function render(container) {
    const options = await loadOptions();

    container.innerHTML = `
        <div class="page-header">
            <h1 class="page-title">Crime Trends</h1>
            <p class="page-subtitle">Temporal analysis, area comparisons, and time-of-day patterns</p>
        </div>
        <div class="tabs" id="trends-tabs">
            <button class="tab active" data-tab="monthly">Monthly Trends</button>
            <button class="tab" data-tab="compare">Area Comparison</button>
            <button class="tab" data-tab="patterns">Time Patterns</button>
        </div>

        <div id="tab-monthly" class="tab-content active">
            <div class="card">
                <div class="card-header">
                    <span class="card-title">Monthly Crime Count</span>
                    <div style="display:flex;gap:8px;">
                        <select id="trend-district" style="width:auto;">
                            <option value="">All Districts</option>
                            ${options.districts.map(d => `<option value="${d.id}">${d.name}</option>`).join('')}
                        </select>
                    </div>
                </div>
                <div class="chart-container" style="height:350px;">
                    <canvas id="monthly-chart"></canvas>
                </div>
            </div>
        </div>

        <div id="tab-compare" class="tab-content">
            <div class="card">
                <div class="card-header"><span class="card-title">Area Comparison</span></div>
                <div class="filter-bar">
                    <div class="filter-group">
                        <label class="filter-label">Area 1</label>
                        <select id="compare-area1">
                            <option value="">City-wide</option>
                            ${options.districts.map(d => `<option value="${d.id}">${d.name}</option>`).join('')}
                        </select>
                    </div>
                    <div class="filter-group">
                        <label class="filter-label">Area 2</label>
                        <select id="compare-area2">
                            <option value="">City-wide</option>
                            ${options.districts.map(d => `<option value="${d.id}">${d.name}</option>`).join('')}
                        </select>
                    </div>
                    <div class="filter-group" style="justify-content:flex-end;">
                        <button class="btn btn-primary" id="compare-btn">Compare</button>
                    </div>
                </div>
                <div class="grid grid-2">
                    <div>
                        <div class="chart-container" style="height:300px;"><canvas id="compare-chart1"></canvas></div>
                        <div class="metric-card" style="margin-top:12px;text-align:center;" id="compare-score1"></div>
                    </div>
                    <div>
                        <div class="chart-container" style="height:300px;"><canvas id="compare-chart2"></canvas></div>
                        <div class="metric-card" style="margin-top:12px;text-align:center;" id="compare-score2"></div>
                    </div>
                </div>
            </div>
        </div>

        <div id="tab-patterns" class="tab-content">
            <div class="card">
                <div class="card-header">
                    <span class="card-title">Day / Hour Heatmap</span>
                    <select id="patterns-year" style="width:auto;">
                        <option value="">All Years</option>
                        ${options.years.map(y => `<option value="${y}">${y}</option>`).join('')}
                    </select>
                </div>
                <div class="chart-container" style="height:350px;">
                    <canvas id="patterns-chart"></canvas>
                </div>
            </div>
        </div>
    `;

    // Tab switching
    document.getElementById('trends-tabs').addEventListener('click', (e) => {
        if (!e.target.classList.contains('tab')) return;
        document.querySelectorAll('#trends-tabs .tab').forEach(t => t.classList.remove('active'));
        e.target.classList.add('active');
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        document.getElementById('tab-' + e.target.dataset.tab).classList.add('active');
    });

    // Monthly chart
    document.getElementById('trend-district').addEventListener('change', loadMonthly);
    await loadMonthly();

    // Compare
    document.getElementById('compare-btn').addEventListener('click', loadCompare);

    // Patterns
    document.getElementById('patterns-year').addEventListener('change', loadPatterns);
    await loadPatterns();
}

async function loadMonthly() {
    const district = document.getElementById('trend-district').value;
    const data = await get('/trends/monthly', { district: district || undefined });
    const monthly = data.monthly;

    destroyCharts();
    const allData = monthly['all'] || Object.values(monthly)[0] || [];
    if (allData.length === 0) return;

    const labels = allData.map(t => `${t.year}-${String(t.month).padStart(2, '0')}`);
    const values = allData.map(t => t.count);

    const chart = createBarChart(
        document.getElementById('monthly-chart'),
        labels, values, { label: 'Crimes' },
    );
    charts.push(chart);
}

async function loadCompare() {
    const a1 = document.getElementById('compare-area1').value;
    const a2 = document.getElementById('compare-area2').value;

    const data = await get('/trends/compare', {
        area1_type: 'district', area1_value: a1,
        area2_type: 'district', area2_value: a2,
    });

    // Chart 1
    const c1Canvas = document.getElementById('compare-chart1');
    const c2Canvas = document.getElementById('compare-chart2');

    // Destroy old compare charts (indices 1,2 if they exist)
    charts.filter(c => c.canvas === c1Canvas || c.canvas === c2Canvas).forEach(c => destroyChart(c));
    charts = charts.filter(c => c.canvas !== c1Canvas && c.canvas !== c2Canvas);

    for (const [area, canvasEl, scoreEl] of [
        [data.area1, c1Canvas, document.getElementById('compare-score1')],
        [data.area2, c2Canvas, document.getElementById('compare-score2')],
    ]) {
        if (area.trend.length > 0) {
            const labels = area.trend.map(t => `${t.year}-${String(t.month).padStart(2, '0')}`);
            const values = area.trend.map(t => t.count);
            const chart = createBarChart(canvasEl, labels, values);
            charts.push(chart);
        }
        scoreEl.innerHTML = `
            <div class="metric-value">${area.severity.toLocaleString()}</div>
            <div class="metric-label">Severity Score</div>
        `;
    }
}

async function loadPatterns() {
    const year = document.getElementById('patterns-year').value;
    const data = await get('/trends/time-patterns', { year: year || undefined });

    // Destroy pattern chart if exists
    const pCanvas = document.getElementById('patterns-chart');
    charts.filter(c => c.canvas === pCanvas).forEach(c => destroyChart(c));
    charts = charts.filter(c => c.canvas !== pCanvas);

    const chart = createHeatmapMatrix(pCanvas, data.matrix);
    charts.push(chart);
}

function destroyCharts() {
    charts.forEach(c => destroyChart(c));
    charts = [];
}

export function destroy() {
    destroyCharts();
}
