import { get } from './api.js';

export const state = {
    options: null,
    filters: {
        year: 'All',
        district: 'All',
        beat: 'All',
        neighborhood: 'All',
    },
};

export async function loadOptions() {
    if (!state.options) {
        state.options = await get('/meta/options');
    }
    return state.options;
}

export function getFilters() {
    return { ...state.filters };
}

export function setFilter(key, value) {
    state.filters[key] = value;
}

export function getFilterParams() {
    const p = {};
    const f = state.filters;
    if (f.year !== 'All') p.year = f.year;
    if (f.district !== 'All') p.district = f.district;
    if (f.beat !== 'All') p.beat = f.beat;
    if (f.neighborhood !== 'All') p.neighborhood = f.neighborhood;
    return p;
}
