const TILE_URL = 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';
const TILE_ATTR = '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>';
const PEORIA_CENTER = [40.6936, -89.5890];

const CRIME_COLORS = {
    'Homicide Offenses': '#d32f2f',
    'Assault Offenses': '#f44336',
    'Robbery': '#e91e63',
    'Weapon Law Violations': '#ff5722',
    'Burglary/Breaking & Entering': '#ff9800',
    'Motor Vehicle Theft': '#ffc107',
    'Larceny/Theft Offenses': '#2196f3',
    'Drug/Narcotic Offenses': '#9c27b0',
    'Destruction/Damage/Vandalism of Property': '#607d8b',
};
const DEFAULT_COLOR = '#757575';

export function crimeColor(offense) {
    return CRIME_COLORS[offense] || DEFAULT_COLOR;
}

export function createMap(containerId, options = {}) {
    const map = L.map(containerId, {
        center: options.center || PEORIA_CENTER,
        zoom: options.zoom || 12,
        zoomControl: true,
        attributionControl: true,
    });
    L.tileLayer(TILE_URL, {
        attribution: TILE_ATTR,
        maxZoom: 19,
        subdomains: 'abcd',
    }).addTo(map);
    return map;
}

export function addHeatmap(map, crimes) {
    const points = crimes
        .filter(c => c.latitude && c.longitude)
        .map(c => [c.latitude, c.longitude, 0.5]);
    if (points.length === 0) return null;
    const heat = L.heatLayer(points, {
        radius: 18,
        blur: 22,
        maxZoom: 15,
        gradient: {
            0.0: '#0a0a0f',
            0.3: '#4a1942',
            0.5: '#e94560',
            0.7: '#ff6b6b',
            1.0: '#ffffff',
        },
    });
    heat.addTo(map);
    return heat;
}

export function addMarkers(map, crimes, maxMarkers = 500) {
    const group = L.layerGroup();
    const subset = crimes.slice(0, maxMarkers);
    for (const c of subset) {
        if (!c.latitude || !c.longitude) continue;
        const color = crimeColor(c.nibrs_offense || '');
        const marker = L.circleMarker([c.latitude, c.longitude], {
            radius: 5,
            color: color,
            fillColor: color,
            fillOpacity: 0.7,
            weight: 1,
        });
        const date = (c.report_date || '').slice(0, 10);
        marker.bindPopup(
            `<b>${c.nibrs_offense || 'Unknown'}</b><br>` +
            `${c.nibrs_description || ''}<br>` +
            `${c.address || ''}<br>` +
            `${date}`
        );
        group.addLayer(marker);
    }
    group.addTo(map);
    return group;
}

export function addBoundaries(map, geojson) {
    if (!geojson || !geojson.features || geojson.features.length === 0) return null;
    const layer = L.geoJSON(geojson, {
        style: {
            fillColor: 'transparent',
            color: '#e94560',
            weight: 1.5,
            opacity: 0.5,
        },
        onEachFeature: (feature, layer) => {
            if (feature.properties && feature.properties.name) {
                layer.bindTooltip(feature.properties.name, {
                    className: 'dark-tooltip',
                    direction: 'center',
                });
            }
        },
    });
    layer.addTo(map);
    return layer;
}

export function destroyMap(map) {
    if (map) {
        map.remove();
    }
}
