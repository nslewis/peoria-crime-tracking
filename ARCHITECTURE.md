# Peoria Crime Tracking Platform — System Architecture

> This document is the single source of truth for rebuilding this system.
> Any AI agent or developer should be able to reconstruct the entire application
> from this document alone, following its rules and specifications.

## Article I: Purpose & Scope

**Section 1.1 — Mission:** Provide Peoria, IL residents with an interactive,
multi-source crime data platform that enables area-level crime exploration,
severity assessment, and historical trend analysis.

**Section 1.2 — Jurisdiction:** This platform covers the City of Peoria, IL
and its surrounding police jurisdictions. Geographic units include Districts,
Beats, Neighborhoods, and individual addresses.

**Section 1.3 — Data Sovereignty:** All data is stored locally in SQLite.
The platform owns its data copy and does not depend on upstream availability
for serving cached data. Fresh data is pulled on-demand from public APIs.

---

## Article II: Data Sources & Ingestion

**Section 2.1 — Primary Source: Peoria PD ArcGIS (No Authentication)**

All endpoints are under the ArcGIS organization `Vm4J3EDyqMzmDYgP` hosted
at `services1.arcgis.com`.

| Dataset | Service Name | Layer | Est. Records |
|---------|-------------|-------|-------------|
| Crimes | `Crimes_public_b259ad13665440579e8fa083818cdd9f` | 0 | 68,000+ |
| Calls for Service | `CallsForService_cc05ba2862d74015aea976e3aefe4f1f` | 0 | 295,000+ |
| ShotSpotter | `ShotSpotter_Dashboard_Data` | 0 | 10,000+ |
| Beat Boundaries | `Beat_cc05ba2862d74015aea976e3aefe4f1f` | 0 | ~20 |
| District Boundaries | `District_cc05ba2862d74015aea976e3aefe4f1f` | 0 | ~5 |
| Community Policing | `CommunityPolicingAreas_public_b259ad13665440579e8fa083818cdd9f` | 0 | ~10 |

**Section 2.2 — Query Protocol:**

```
GET {ARCGIS_BASE}/{service_name}/FeatureServer/0/query
Parameters:
  where=1=1          (or filter expression)
  outFields=*        (all fields)
  outSR=4326         (WGS84 lat/lon)
  f=json             (or geojson)
  resultRecordCount=2000  (page size, max allowed)
  resultOffset=N     (pagination offset)
```

Pagination: increment `resultOffset` by 2000 until `exceededTransferLimit` is false
or features array is empty.

**Section 2.3 — Field Mappings (Crimes):**

| ArcGIS Field | Database Column | Type |
|-------------|----------------|------|
| offenseid | offense_id | TEXT UNIQUE |
| callid | call_id | TEXT |
| statute | statute | TEXT |
| nibrscode | nibrs_code | TEXT |
| nibrsoffense | nibrs_offense | TEXT |
| nibrsdesc | nibrs_description | TEXT |
| nibrscrimeag | crime_against | TEXT |
| attemptcompleted | attempt_completed | TEXT |
| fulladdr | address | TEXT |
| city | city | TEXT |
| state | state | TEXT |
| zip5 | zip | TEXT |
| beat | beat | TEXT |
| district | district | TEXT |
| neighborhood | neighborhood | TEXT |
| weaponcat | weapon_category | TEXT |
| weapondesc | weapon_description | TEXT |
| reportdate | report_date | TEXT (ISO 8601, converted from ms timestamp) |
| reportyear | report_year | INTEGER |
| reportmonth | report_month | INTEGER |
| reporthour | report_hour | INTEGER |
| reportdow | report_dow | TEXT |
| geometry.y | latitude | REAL |
| geometry.x | longitude | REAL |

**Section 2.4 — Timestamp Convention:**

ArcGIS returns dates as Unix milliseconds (e.g., `1687848000000`).
Convert to ISO 8601 UTC strings for storage: `datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()`

---

## Article III: Database Schema

**Section 3.1 — Engine:** SQLite with WAL journal mode and foreign keys enabled.

**Section 3.2 — Tables:**

```sql
crimes (id PK, offense_id UNIQUE, call_id, statute, nibrs_code, nibrs_offense,
        nibrs_description, crime_against, attempt_completed, address, city,
        state, zip, beat, district, neighborhood, weapon_category,
        weapon_description, report_date, report_year, report_month,
        report_hour, report_dow, latitude, longitude, source, synced_at)

calls_for_service (id PK, call_id UNIQUE, call_type, priority, disposition,
                   address, beat, district, call_date, latitude, longitude,
                   source, synced_at)

shotspotter (id PK, incident_id UNIQUE, rounds_fired, event_type, address,
             beat, district, event_date, latitude, longitude, source, synced_at)

boundaries (id PK, boundary_type, name, geometry_geojson,
            UNIQUE(boundary_type, name))

sync_log (id PK, source, table_name, records_fetched, started_at,
          completed_at, status)
```

**Section 3.3 — Indexes:**

- crimes: report_date, report_year, district, beat, neighborhood, nibrs_offense, (latitude, longitude)
- calls_for_service: call_date, district
- shotspotter: event_date
- boundaries: boundary_type

**Section 3.4 — Duplicate Handling:** `INSERT OR IGNORE` using UNIQUE constraints
on natural keys (offense_id, call_id, incident_id). Boundaries use `INSERT OR REPLACE`.

---

## Article IV: Application Architecture

**Section 4.1 — Technology Stack:**

| Component | Technology | Version |
|-----------|-----------|---------|
| Frontend | Streamlit | >=1.30.0 |
| Maps | Folium + streamlit-folium | >=0.15.0 / >=0.17.0 |
| Charts | Plotly Express | >=5.18.0 |
| Data | Pandas | >=2.0.0 |
| HTTP | Requests | >=2.31.0 |
| Database | SQLite3 (stdlib) | — |
| Python | CPython | >=3.10 |

**Section 4.2 — Module Structure:**

```
peoria_crime_tracking/
  app.py                    # Streamlit entry point, page routing
  requirements.txt          # Python dependencies
  ARCHITECTURE.md           # This document
  src/
    __init__.py
    config.py               # Endpoints, weights, paths
    database.py             # init_db(), get_connection()
    sync.py                 # ArcGIS fetching, data insertion
    queries.py              # Query engine, scoring, trends
    map_utils.py            # Folium map creation and overlays
    pages/
      __init__.py
      dashboard.py          # Page 1: overview with map + score
      explore.py            # Page 2: data table + filters + export
      trends.py             # Page 3: charts + comparison
      sync_page.py          # Page 4: sync controls + transparency
  tests/
    __init__.py
    test_database.py
    test_sync.py
    test_queries.py
    test_app_smoke.py
  docs/
    plans/                  # Design and implementation documents
```

**Section 4.3 — Data Flow:**

```
ArcGIS REST APIs ──fetch_all_records()──> sync_crimes/calls/shot/boundaries()
        │                                         │
        │                                   INSERT OR IGNORE
        │                                         │
        ▼                                         ▼
  Public Internet                          SQLite Database
                                                  │
                                          get_connection()
                                                  │
                              ┌────────────┬──────┴───────┬──────────────┐
                              ▼            ▼              ▼              ▼
                         queries.py   map_utils.py   dashboard.py   explore.py
                              │            │              │              │
                              └────────────┴──────────────┴──────────────┘
                                                  │
                                           Streamlit UI
```

---

## Article V: Crime Severity Scoring

**Section 5.1 — Weight Table:**

| NIBRS Offense Category | Weight |
|----------------------|--------|
| Homicide Offenses | 10 |
| Robbery | 7 |
| Kidnapping/Abduction | 7 |
| Sex Offenses | 6 |
| Assault Offenses | 5 |
| Arson | 5 |
| Burglary/Breaking & Entering | 4 |
| Weapon Law Violations | 4 |
| Motor Vehicle Theft | 3 |
| Larceny/Theft Offenses | 2 |
| Drug/Narcotic Offenses | 2 |
| Fraud Offenses | 2 |
| Stolen Property Offenses | 2 |
| Destruction/Damage/Vandalism | 1 |
| Trespass of Real Property | 1 |
| Disorderly Conduct | 1 |
| All others | 1 (DEFAULT_WEIGHT) |

**Section 5.2 — Score Formula:**

```
area_score = SUM(weight[offense_type] * count[offense_type]) for all offenses in area
city_avg = total_city_score / number_of_districts
ratio = area_score / city_avg
```

**Section 5.3 — Rating Bands:**

| Ratio | Rating | Color |
|-------|--------|-------|
| < 0.5 | Low | #4caf50 (green) |
| 0.5–1.0 | Moderate | #ff9800 (orange) |
| 1.0–1.5 | High | #f44336 (red) |
| > 1.5 | Very High | #b71c1c (dark red) |

---

## Article VI: UI Specification

**Section 6.1 — Page 1: Dashboard**
- 4-column filter bar: Year, District, Beat, Neighborhood
- 3-column metrics: Severity score card (color-coded), total crimes count, top 3 crime types
- Interactive Folium map: heatmap layer + circle markers (max 300) + district boundary overlay
- Monthly bar chart (Plotly)

**Section 6.2 — Page 2: Explore Data**
- Radio toggle: Crimes / Calls for Service / ShotSpotter
- Filters: crime type, district (for crimes table)
- Folium map with markers (max 500)
- Pandas dataframe display (max 5000 rows)
- CSV download button

**Section 6.3 — Page 3: Trends & Comparison**
- Tab 1 (Monthly): multi-year line chart + stacked area by crime type (top 8)
- Tab 2 (Comparison): side-by-side district selector, overlaid line chart, score metrics
- Tab 3 (Time Patterns): day-of-week × hour-of-day heatmap (px.imshow, YlOrRd colorscale)

**Section 6.4 — Page 4: Data Sources & Sync**
- 4-column metric cards showing record counts per table
- Sync history dataframe (last 20 entries from sync_log)
- Full Sync button (primary) + single-source selector with Sync button
- Endpoint URLs displayed as code blocks
- Score methodology table

---

## Article VII: Operational Rules

**Section 7.1 — Sync Behavior:**
- Full sync calls: sync_crimes → sync_calls_for_service → sync_shotspotter → sync_boundaries
- Each sync function fetches ALL records (paginated at 2000/page)
- Duplicates are silently skipped via INSERT OR IGNORE
- Each sync logs to sync_log with record count and timestamps
- init_db() is called before sync to ensure schema exists

**Section 7.2 — Map Defaults:**
- Center: (40.6936, -89.5890) — downtown Peoria
- Zoom: 12
- Tiles: CartoDB positron
- Heatmap: radius=15, blur=20, max_zoom=15
- Markers: CircleMarker, radius=5, color by offense type, max 300-500

**Section 7.3 — Query Safety:**
- All value parameters use ? placeholders (parameterized queries)
- Column names (area_type) come from application code, never user input
- Connection uses row_factory=sqlite3.Row for dict-like access

**Section 7.4 — Color Scheme for Crime Markers:**

| Offense | Color |
|---------|-------|
| Homicide | #d32f2f |
| Assault | #f44336 |
| Robbery | #e91e63 |
| Weapons | #ff5722 |
| Burglary | #ff9800 |
| Vehicle Theft | #ffc107 |
| Theft | #2196f3 |
| Drugs | #9c27b0 |
| Vandalism | #607d8b |
| Other | #757575 |
