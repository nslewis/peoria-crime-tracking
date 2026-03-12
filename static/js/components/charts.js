// Chart.js dark theme defaults
Chart.defaults.color = '#a0a0b0';
Chart.defaults.borderColor = 'rgba(42,42,74,0.5)';
Chart.defaults.font.family = "'Inter', sans-serif";

const CHART_COLORS = [
    '#e94560', '#4fc3f7', '#53d769', '#f0c040', '#9c27b0',
    '#ff5722', '#2196f3', '#ff9800', '#607d8b', '#e91e63',
];

export function createBarChart(canvas, labels, data, options = {}) {
    return new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: options.label || 'Count',
                data,
                backgroundColor: options.color || 'rgba(233,69,96,0.7)',
                borderColor: options.color || '#e94560',
                borderWidth: 1,
                borderRadius: 3,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1a1a2e',
                    borderColor: '#2a2a4a',
                    borderWidth: 1,
                    titleFont: { family: "'Inter', sans-serif" },
                    bodyFont: { family: "'JetBrains Mono', monospace", size: 12 },
                },
            },
            scales: {
                x: { grid: { display: false } },
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(42,42,74,0.3)' },
                    ticks: { font: { family: "'JetBrains Mono', monospace", size: 11 } },
                },
            },
        },
    });
}

export function createLineChart(canvas, datasets, labels, options = {}) {
    const chartDatasets = datasets.map((ds, i) => ({
        label: ds.label,
        data: ds.data,
        borderColor: ds.color || CHART_COLORS[i % CHART_COLORS.length],
        backgroundColor: 'transparent',
        borderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 5,
        tension: 0.3,
    }));
    return new Chart(canvas, {
        type: 'line',
        data: { labels, datasets: chartDatasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: datasets.length > 1, position: 'top', labels: { boxWidth: 12 } },
                tooltip: {
                    backgroundColor: '#1a1a2e',
                    borderColor: '#2a2a4a',
                    borderWidth: 1,
                    bodyFont: { family: "'JetBrains Mono', monospace", size: 12 },
                },
            },
            scales: {
                x: { grid: { display: false } },
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(42,42,74,0.3)' },
                    ticks: { font: { family: "'JetBrains Mono', monospace", size: 11 } },
                },
            },
        },
    });
}

export function createDoughnutChart(canvas, labels, data, options = {}) {
    return new Chart(canvas, {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: CHART_COLORS.slice(0, data.length),
                borderColor: '#1a1a2e',
                borderWidth: 2,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '60%',
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        boxWidth: 12,
                        padding: 12,
                        font: { size: 11 },
                    },
                },
                tooltip: {
                    backgroundColor: '#1a1a2e',
                    borderColor: '#2a2a4a',
                    borderWidth: 1,
                    bodyFont: { family: "'JetBrains Mono', monospace", size: 12 },
                },
            },
        },
    });
}

export function createHeatmapMatrix(canvas, matrix, options = {}) {
    const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const hours = Array.from({ length: 24 }, (_, i) => `${i}:00`);

    // Build flat data array
    const data = [];
    let maxVal = 0;
    for (let d = 0; d < days.length; d++) {
        const dayData = matrix[days[d]] || {};
        for (let h = 0; h < 24; h++) {
            const val = dayData[String(h)] || 0;
            if (val > maxVal) maxVal = val;
            data.push({ x: h, y: d, v: val });
        }
    }

    return new Chart(canvas, {
        type: 'scatter',
        data: {
            datasets: [{
                data: data.map(d => ({ x: d.x, y: d.y })),
                backgroundColor: data.map(d => {
                    const intensity = maxVal > 0 ? d.v / maxVal : 0;
                    if (intensity === 0) return 'rgba(26,26,46,0.5)';
                    const r = Math.round(26 + (233 - 26) * intensity);
                    const g = Math.round(26 + (69 - 26) * intensity);
                    const b = Math.round(46 + (96 - 46) * intensity);
                    return `rgba(${r},${g},${b},${0.3 + intensity * 0.7})`;
                }),
                pointRadius: data.map(d => {
                    const intensity = maxVal > 0 ? d.v / maxVal : 0;
                    return 4 + intensity * 10;
                }),
                pointHoverRadius: 12,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1a1a2e',
                    borderColor: '#2a2a4a',
                    borderWidth: 1,
                    callbacks: {
                        label: (ctx) => {
                            const d = data[ctx.dataIndex];
                            return `${days[d.y]} ${d.x}:00 — ${d.v} crimes`;
                        },
                    },
                },
            },
            scales: {
                x: {
                    min: -0.5,
                    max: 23.5,
                    ticks: {
                        callback: (v) => `${v}:00`,
                        font: { family: "'JetBrains Mono', monospace", size: 10 },
                    },
                    grid: { color: 'rgba(42,42,74,0.2)' },
                },
                y: {
                    min: -0.5,
                    max: 6.5,
                    ticks: {
                        callback: (v) => days[v] ? days[v].slice(0, 3) : '',
                        font: { family: "'JetBrains Mono', monospace", size: 10 },
                    },
                    grid: { color: 'rgba(42,42,74,0.2)' },
                    reverse: true,
                },
            },
        },
    });
}

export function destroyChart(chart) {
    if (chart) chart.destroy();
}
