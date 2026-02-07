# Peoria Crime Tracking Platform — Design Document

## Purpose

A Streamlit application that aggregates crime data from multiple Peoria, IL sources into a local SQLite database, providing residents with an interactive way to explore crime in their area, see how it compares to other areas, and track changes over time.

## Data Sources

### Primary: Peoria PD ArcGIS (no auth required)

| Dataset | Endpoint | Records |
|---------|----------|---------|
| Crimes | `services1.arcgis.com/Vm4J3EDyqMzmDYgP/arcgis/rest/services/Crimes_public_.../FeatureServer/0` | 68,215+ |
| Calls for Service | `services1.arcgis.com/Vm4J3EDyqMzmDYgP/arcgis/rest/services/CallsForService_.../FeatureServer/0` | 295,103 |
| ShotSpotter | `services1.arcgis.com/Vm4J3EDyqMzmDYgP/arcgis/rest/services/ShotSpotter_Dashboard_Data/FeatureServer/0` | 10,582 |
| Boundaries | Beat, District, Division, Community Policing Area layers | — |

### Secondary: FBI Crime Data Explorer (free API key)

For national/regional comparison context.

## Architecture

```
Streamlit Frontend (folium maps, plotly charts)
        │
Data Service Layer (query engine, score calculator, trend analyzer)
        │
SQLite Database (crimes, calls, shotspotter, boundaries, sync_log)
        │
Data Sync Module (ArcGIS REST, FBI API)
```

## Database Schema

### crimes
offense_id, call_id, statute, nibrs_code, nibrs_offense, nibrs_description, crime_against, attempt_completed, address, city, state, zip, beat, district, neighborhood, weapon_category, weapon_description, report_date, report_year, report_month, report_hour, report_dow, latitude, longitude, source, synced_at

### calls_for_service
call_id, call_type, priority, disposition, address, beat, district, call_date, latitude, longitude, source, synced_at

### shotspotter
incident_id, rounds_fired, event_type, address, beat, district, event_date, latitude, longitude, source, synced_at

### boundaries
boundary_type (beat/district/neighborhood/community_policing), name, geometry_geojson

### sync_log
source, table_name, records_fetched, started_at, completed_at, status

## Crime Severity Score

Computed on-the-fly, not stored. Uses weighted crime counts:
- Homicide: 10, Robbery: 7, Assault: 5, Burglary: 4, Weapon violations: 4
- Vehicle theft: 3, Theft: 2, Drug offenses: 2, Vandalism: 1, Other: 1

Normalized per area and compared to city-wide average. Displayed as color-coded rating (green/yellow/orange/red).

## UI Pages

### Page 1: Dashboard
- Date range picker, area selector (District > Beat > Neighborhood > Address)
- Interactive map with heatmap, markers, boundary overlays
- Crime severity score card with trend arrow
- Summary stats: total crimes, top crime types, change vs previous period

### Page 2: Explore Data
- Toggle data sources as layers (Crimes, Calls, ShotSpotter)
- Filters: crime type, date, day of week, hour, weapon
- Table view with CSV export
- Click-to-map linking

### Page 3: Trends & Comparison
- Month-over-month and year-over-year charts
- Side-by-side area comparison
- Crime type breakdown over time
- Day/hour heatmaps

### Page 4: Data Sources & Sync
- Sync status per source
- Manual sync button
- Methodology transparency

## Technical Decisions

- **SQLite** for local-first storage, structured for future PostgreSQL migration
- **Folium** for maps (Leaflet-based, Streamlit compatible)
- **Plotly** for interactive charts
- **Daily auto-sync + manual refresh** for data freshness
- **ArcGIS pagination** at 2000 records per request with outSR=4326 for WGS84 coordinates
