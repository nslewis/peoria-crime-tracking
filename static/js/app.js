import { initRouter } from './router.js';
import { loadOptions } from './state.js';

async function init() {
    // Hamburger toggle
    const toggle = document.getElementById('nav-toggle');
    const links = document.getElementById('nav-links');
    toggle.addEventListener('click', () => {
        links.classList.toggle('open');
        toggle.classList.toggle('open');
    });

    // Close nav on link click (mobile)
    links.addEventListener('click', (e) => {
        if (e.target.classList.contains('nav-link')) {
            links.classList.remove('open');
            toggle.classList.remove('open');
        }
    });

    // Pre-load filter options
    await loadOptions();

    // Start router
    const content = document.getElementById('app-content');
    initRouter(content);
}

init();
