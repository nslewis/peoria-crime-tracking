const BASE = '/api';

export async function get(path, params = {}) {
    const url = new URL(BASE + path, window.location.origin);
    for (const [k, v] of Object.entries(params)) {
        if (v !== null && v !== undefined && v !== '' && v !== 'All') {
            url.searchParams.set(k, v);
        }
    }
    const resp = await fetch(url);
    if (!resp.ok) {
        throw new Error(`API ${resp.status}: ${resp.statusText}`);
    }
    return resp.json();
}

export async function post(path, params = {}) {
    const url = new URL(BASE + path, window.location.origin);
    for (const [k, v] of Object.entries(params)) {
        if (v !== null && v !== undefined && v !== '') {
            url.searchParams.set(k, v);
        }
    }
    const resp = await fetch(url, { method: 'POST' });
    if (!resp.ok) {
        throw new Error(`API ${resp.status}: ${resp.statusText}`);
    }
    return resp.json();
}
