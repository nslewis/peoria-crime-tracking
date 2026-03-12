import { state, setFilter, loadOptions } from '../state.js';

export async function renderFilterBar(container, onChange) {
    const options = await loadOptions();
    const bar = document.createElement('div');
    bar.className = 'filter-bar';

    bar.innerHTML = `
        <div class="filter-group">
            <label class="filter-label">Year</label>
            <select id="filter-year">
                <option value="All">All Years</option>
                ${options.years.map(y => `<option value="${y}" ${state.filters.year === y ? 'selected' : ''}>${y}</option>`).join('')}
            </select>
        </div>
        <div class="filter-group">
            <label class="filter-label">District</label>
            <select id="filter-district">
                <option value="All">All Districts</option>
                ${options.districts.map(d => `<option value="${d.id}" ${state.filters.district === d.id ? 'selected' : ''}>${d.name}</option>`).join('')}
            </select>
        </div>
        <div class="filter-group">
            <label class="filter-label">Beat</label>
            <select id="filter-beat">
                <option value="All">All Beats</option>
                ${options.beats.map(b => `<option value="${b}" ${state.filters.beat === b ? 'selected' : ''}>${b}</option>`).join('')}
            </select>
        </div>
        <div class="filter-group">
            <label class="filter-label">Neighborhood</label>
            <select id="filter-neighborhood">
                <option value="All">All Neighborhoods</option>
                ${options.neighborhoods.map(n => `<option value="${n}" ${state.filters.neighborhood === n ? 'selected' : ''}>${n}</option>`).join('')}
            </select>
        </div>
    `;

    container.appendChild(bar);

    // Bind change events
    for (const key of ['year', 'district', 'beat', 'neighborhood']) {
        const sel = bar.querySelector(`#filter-${key}`);
        sel.addEventListener('change', () => {
            setFilter(key, sel.value);
            if (onChange) onChange();
        });
    }
}
