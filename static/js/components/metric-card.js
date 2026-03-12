export function createMetricCard({ value, label, sub, color, severity } = {}) {
    const div = document.createElement('div');
    div.className = 'metric-card';
    if (severity === 'High' || severity === 'Very High') {
        div.classList.add('severity-high');
    } else if (severity === 'Low') {
        div.classList.add('severity-low');
    }
    div.innerHTML = `
        <div class="metric-value" style="${color ? `color: ${color}` : ''}">${value}</div>
        <div class="metric-label">${label}</div>
        ${sub ? `<div class="metric-sub">${sub}</div>` : ''}
    `;
    return div;
}

export function createChangeIndicator(changePct) {
    if (changePct === null || changePct === undefined) return '—';
    const arrow = changePct > 0 ? '&#9650;' : changePct < 0 ? '&#9660;' : '&#9644;';
    const colorClass = changePct > 0 ? 'badge-danger' : changePct < 0 ? 'badge-success' : 'badge-info';
    return `<span class="badge ${colorClass}">${arrow} ${Math.abs(changePct).toFixed(1)}%</span>`;
}
