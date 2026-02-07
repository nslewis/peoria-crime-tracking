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
    "Kidnapping/Abduction Offenses": 7,
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
    "Counterfeiting/Forgery": 2,
    "Destruction/Damage/Vandalism of Property": 1,
    "Trespass of Real Property": 1,
    "Disorderly Conduct": 1,
    "Driving Under the Influence": 2,
    "Liquor Law Violations": 1,
    "Family Offenses, Nonviolent": 2,
    "Pornography/Obscene Material": 3,
    "Curfew/Loitering/Vagrancy Violations": 1,
    "Runaway": 1,
    "Other": 1,
}
DEFAULT_WEIGHT = 1

# Human-readable names for Peoria PD district numbers
DISTRICT_NAMES = {
    "1": "Downtown / Riverfront",
    "2": "South Side / Warehouse District",
    "3": "West Bluff / Bradley University",
    "4": "South Side West / Bartonville",
    "5": "East Bluff / Glen Oak",
    "6": "Central Peoria / Knoxville",
    "8": "South Side Central / Western Ave",
    "10": "North Valley / Averyville",
    "11": "Moss-Bradley / Upper West Bluff",
    "12": "East Bluff North / Prospect Rd",
    "13": "Sterling / Heading",
    "14": "Richwoods / Northmoor",
    "15": "North Peoria / Allen Rd",
    "16": "Far North / Chillicothe",
    "17": "Far Northeast / Peoria Heights",
    "18": "University / Forrest Hill",
    "19": "Far Northwest / Kickapoo",
}
