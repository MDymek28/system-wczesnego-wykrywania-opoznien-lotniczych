import math
from pathlib import Path

import pandas as pd


# === PLIKI ===
BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = BASE_DIR / "lot_flights_only.xlsx"
OUTPUT_FILE = BASE_DIR / "lot_flights_only_poland_with_airports_abroad.xlsx"

MAX_DISTANCE_KM = 10

# === LOTNISKA IFR W POLSCE ===
AIRPORTS = {
    "EPBY": {"name": "Bydgoszcz", "lat": 53.096802, "lon": 17.977699},
    "EPGD": {"name": "Gdańsk", "lat": 54.377602, "lon": 18.466200},
    "EPKK": {"name": "Kraków", "lat": 50.077702, "lon": 19.784800},
    "EPKT": {"name": "Katowice", "lat": 50.474300, "lon": 19.080000},
    "EPLB": {"name": "Lublin", "lat": 51.240278, "lon": 22.713611},
    "EPLL": {"name": "Łódź", "lat": 51.721944, "lon": 19.398056},
    "EPMO": {"name": "Warszawa Modlin", "lat": 52.451111, "lon": 20.651667},
    "EPRA": {"name": "Radom", "lat": 51.389167, "lon": 21.213056},
    "EPRZ": {"name": "Rzeszów", "lat": 50.109958, "lon": 22.019000},
    "EPSC": {"name": "Szczecin", "lat": 53.584722, "lon": 14.902222},
    "EPSY": {"name": "Olsztyn-Mazury", "lat": 53.481944, "lon": 20.937222},
    "EPWA": {"name": "Warszawa Chopin", "lat": 52.165833, "lon": 20.967222},
    "EPWR": {"name": "Wrocław", "lat": 51.102778, "lon": 16.885833},
    "EPZG": {"name": "Zielona Góra", "lat": 52.138517, "lon": 15.798556},
}

# === FUNKCJE ===
# ODLEGLOSC POMIEDZY PUNKTAMI
def haversine_km(lat1, lon1, lat2, lon2):

    r = 6371.0

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )

    c = 2 * math.asin(math.sqrt(a))
    return r * c


def nearest_airport(lat, lon):

    best_code = None
    best_name = None
    best_dist = None

    for code, airport in AIRPORTS.items():
        dist = haversine_km(lat, lon, airport["lat"], airport["lon"])

        if best_dist is None or dist < best_dist:
            best_code = code
            best_name = airport["name"]
            best_dist = dist

    if best_dist is None:
        return "ABROAD", "ABROAD", None

    if best_dist > MAX_DISTANCE_KM:
        return "ABROAD", "ABROAD", round(best_dist, 2)

    return best_code, best_name, round(best_dist, 2)

# === DOPASOWANIE LOTNISK ===
def match_quality(airport_code, distance_km):

    if airport_code == "ABROAD":
        return "abroad"

    if pd.isna(distance_km):
        return "unknown"

    if distance_km <= 10:
        return "airport"

    return "weak"

# === KLASYFIKACJA LOTU ===
def classify_flight(row):

    dep = row["departure_airport_icao"]
    arr = row["arrival_airport_icao"]

    if dep != "ABROAD" and arr != "ABROAD":
        return "DOMESTIC_OR_POLAND_ONLY"

    if dep == "ABROAD" and arr != "ABROAD":
        return "INBOUND_TO_POLAND"

    if dep != "ABROAD" and arr == "ABROAD":
        return "OUTBOUND_FROM_POLAND"

    return "ABROAD_OR_OVERFLIGHT"


# === WCZYTANIE ===

print("Szukam pliku wejsciowego:")
print(INPUT_FILE)

if not INPUT_FILE.exists():
    raise FileNotFoundError(f"Nie znaleziono pliku wejsciowego: {INPUT_FILE}")

df = pd.read_excel(INPUT_FILE)

print("Kolumny w pliku:")
print(df.columns.tolist())

required_columns = ["callsign", "lat", "lon"]

for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"Brakuje wymaganej kolumny: {col}")

df = df.dropna(subset=["callsign", "lat", "lon"]).copy()
df["callsign"] = df["callsign"].astype(str).str.strip()

if "time" in df.columns:
    df = df.sort_values(["callsign", "time"]).reset_index(drop=True)
else:
    print("UWAGA: brak kolumny 'time'. Uzywam biezącej kolejnosci wierszy.")
    df = df.reset_index(drop=True)


# === 1 LOT = 1 WIERSZ ===

grouped = df.groupby("callsign", as_index=False).agg(
    first_lat=("lat", "first"),
    first_lon=("lon", "first"),
    last_lat=("lat", "last"),
    last_lon=("lon", "last"),
    points_count=("callsign", "size"),
)

dep_data = grouped.apply(
    lambda row: pd.Series(nearest_airport(row["first_lat"], row["first_lon"])),
    axis=1,
)

dep_data.columns = [
    "departure_airport_icao",
    "departure_airport_name",
    "departure_distance_km",
]

arr_data = grouped.apply(
    lambda row: pd.Series(nearest_airport(row["last_lat"], row["last_lon"])),
    axis=1,
)

arr_data.columns = [
    "arrival_airport_icao",
    "arrival_airport_name",
    "arrival_distance_km",
]

grouped = pd.concat([grouped, dep_data, arr_data], axis=1)



# === JAKOSC DOPASOWANIA I TYP LOTU ===

grouped["departure_match_quality"] = grouped.apply(
    lambda row: match_quality(
        row["departure_airport_icao"],
        row["departure_distance_km"],
    ),
    axis=1,
)

grouped["arrival_match_quality"] = grouped.apply(
    lambda row: match_quality(
        row["arrival_airport_icao"],
        row["arrival_distance_km"],
    ),
    axis=1,
)

grouped["flight_type"] = grouped.apply(classify_flight, axis=1)


# === STATYSTYKI KONTROLNE ===

total_flights = len(grouped)

domestic_flights = len(
    grouped[grouped["flight_type"] == "DOMESTIC_OR_POLAND_ONLY"]
)

inbound_flights = len(
    grouped[grouped["flight_type"] == "INBOUND_TO_POLAND"]
)

outbound_flights = len(
    grouped[grouped["flight_type"] == "OUTBOUND_FROM_POLAND"]
)

abroad_or_overflight = len(
    grouped[grouped["flight_type"] == "ABROAD_OR_OVERFLIGHT"]
)

print()
print("Podsumowanie klasyfikacji lotow:")
print(f"Liczba wszystkich lotow: {total_flights}")
print(f"Loty krajowe / w granicach analizowanych lotnisk: {domestic_flights}")
print(f"Loty przylatujace do Polski: {inbound_flights}")
print(f"Loty wylatujace z Polski: {outbound_flights}")
print(f"Loty zagraniczne / przelotowe / poza analizowanymi lotniskami: {abroad_or_overflight}")


# === DOPISANIE DO TABELI ===

result = df.merge(
    grouped[
        [
            "callsign",
            "departure_airport_icao",
            "departure_airport_name",
            "departure_distance_km",
            "arrival_airport_icao",
            "arrival_airport_name",
            "arrival_distance_km",
            #"departure_match_quality",
            #"arrival_match_quality",
            #"flight_type",
        ]
    ],
    on="callsign",
    how="left",
)


# === ZAPIS ===

result.to_excel(OUTPUT_FILE, index=False)

print()
print("Gotowe.")
print(f"Zapisano plik wynikowy: {OUTPUT_FILE}")
print(f"Liczba wierszy w pliku wynikowym: {len(result)}")
print()
print("Dodane kolumny:")
print([
    "departure_airport_icao",
    "departure_airport_name",
    "departure_distance_km",
    "arrival_airport_icao",
    "arrival_airport_name",
    "arrival_distance_km",
    #"departure_match_quality",
    #"arrival_match_quality",
    #"flight_type",
])