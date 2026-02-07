from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "peoria_crime.db"

ARCGIS_BASE = "https://services1.arcgis.com/Vm4J3EDyqMzmDYgP/arcgis/rest/services"

ENDPOINTS = {
    "crimes": f"{ARCGIS_BASE}/Crimes_public_b259ad13665440579e8fa083818cdd9f/FeatureServer/0",
    "calls_for_service": f"{ARCGIS_BASE}/CallsForService_cc05ba2862d74015aea976e3aefe4f1f/FeatureServer/0",
    "shotspotter": f"{ARCGIS_BASE}/ShotSpotter_Dashboard_Data/FeatureServer/0",
    "beats": f"{ARCGIS_BASE}/Beat_cc05ba2862d74015aea976e3aefe4f1f/FeatureServer/0",
    "districts": f"{ARCGIS_BASE}/District_cc05ba2862d74015aea976e3aefe4f1f/FeatureServer/0",
    "community_policing": f"{ARCGIS_BASE}/CommunityPolicingAreas_public_b259ad13665440579e8fa083818cdd9f/FeatureServer/0",
}

PAGE_SIZE = 2000

CRIME_WEIGHTS = {
    "Homicide Offenses": 10,
    "Robbery": 7,
    "Kidnapping/Abduction": 7,
    "Sex Offenses": 6,
    "Assault Offenses": 5,
    "Arson": 5,
    "Burglary/Breaking & Entering": 4,
    "Weapon Law Violations": 4,
    "Motor Vehicle Theft": 3,
    "Larceny/Theft Offenses": 2,
    "Drug/Narcotic Offenses": 2,
    "Fraud Offenses": 2,
    "Stolen Property Offenses": 2,
    "Destruction/Damage/Vandalism": 1,
    "Trespass of Real Property": 1,
    "Disorderly Conduct": 1,
}
DEFAULT_WEIGHT = 1
