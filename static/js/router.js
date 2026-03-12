const routes = {
    dashboard:      () => import('./pages/dashboard.js'),
    streets:        () => import('./pages/street-search.js'),
    explore:        () => import('./pages/explore.js'),
    trends:         () => import('./pages/trends.js'),
    sync:           () => import('./pages/sync.js'),
};

let currentPage = null;
let container = null;

export function initRouter(el) {
    container = el;
    window.addEventListener('hashchange', navigate);
    navigate();
}

async function navigate() {
    const hash = (window.location.hash || '#dashboard').slice(1);
    const pageName = routes[hash] ? hash : 'dashboard';

    // Update active nav link
    document.querySelectorAll('.nav-link').forEach(a => {
        a.classList.toggle('active', a.dataset.page === pageName);
    });

    // Destroy previous page
    if (currentPage && currentPage.destroy) {
        currentPage.destroy();
    }

    // Load new page
    container.innerHTML = '<div class="loading-screen"><div class="loading-pulse"></div><p>Loading...</p></div>';
    try {
        const mod = await routes[pageName]();
        container.innerHTML = '';
        currentPage = mod;
        await mod.render(container);
    } catch (err) {
        container.innerHTML = `<div class="error-message">Failed to load page: ${err.message}</div>`;
        console.error(err);
    }
}

export function navigateTo(page) {
    window.location.hash = '#' + page;
}
