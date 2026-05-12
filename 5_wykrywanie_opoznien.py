import os
import re
from datetime import timedelta

import numpy as np
import pandas as pd

# === PLIKI ===
FLIGHTS_FILE = "lot_flights_only_poland_with_airports_abroad.xlsx"
METAR_FILE = "METAR.xlsx"

OUTPUT_DIR = "output"
LIVE_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "live_risk_map.csv")
HISTORY_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "risk_history.csv")


# False = bez czekania 60 sekund
# True = czekanie 60 sekund miedzy minutami
REALTIME_MODE = False
SLEEP_SECONDS = 60

# None -> caly zakres danych
MAX_MINUTES_TO_PROCESS = None

RUN_ID = "SIM_TEST_001"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def is_polish_airport_code(value):
    value = normalize_text(value)
    return value != "ABROAD" and len(value) == 4


def parse_datetime(date_series, time_series):
    dt_str = date_series.astype(str).str.strip() + " " + time_series.astype(str).str.strip()
    return pd.to_datetime(dt_str, errors="coerce")


def safe_int(value, default=0):
    try:
        if pd.isna(value):
            return default
        return int(value)
    except Exception:
        return default


def level_from_score(score):
    if score is None or pd.isna(score):
        return "BRAK RYZYKA"
    if score <= 2:
        return "BRAK RYZYKA"
    elif score <= 5:
        return "NISKIE RYZYKO"
    elif score <= 8:
        return "PODWYŻSZONE RYZYKO"
    else:
        return "WYSOKIE RYZYKO"


def no_risk_result(reason="Lot poza zakresem analizy METAR"):
    return {
        "score": 0,
        "level": "BRAK RYZYKA",
        "reason": reason,
        "raw": None,
        "metar_time": None
    }


# === WCZYTANIE I PRZYGOTOWANIE LOTOW ===

def load_and_prepare_flights(filepath):
    df = pd.read_excel(filepath)

    # utworzenie datetime
    df["flight_datetime_utc"] = parse_datetime(df["utc_date"], df["utc_time"])

    # czyszczenie tekstow
    df["callsign"] = df["callsign"].astype(str).str.strip()

    df["departure_airport_icao"] = (
        df["departure_airport_icao"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df["arrival_airport_icao"] = (
        df["arrival_airport_icao"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # usuniecie blednych rekordow
    df = df.dropna(subset=["flight_datetime_utc", "lat", "lon"])
    df = df[df["callsign"].str.len() > 0]

    # WAZNE:
    valid_departure = (
        (df["departure_airport_icao"] == "ABROAD")
        | (df["departure_airport_icao"].str.len() == 4)
    )

    valid_arrival = (
        (df["arrival_airport_icao"] == "ABROAD")
        | (df["arrival_airport_icao"].str.len() == 4)
    )

    df = df[valid_departure & valid_arrival].copy()

    # zaokraglenie do minuty do grupowania
    df["minute_bucket"] = df["flight_datetime_utc"].dt.floor("min")

    # sortowanie
    df = df.sort_values(
        ["minute_bucket", "callsign", "flight_datetime_utc"]
    ).reset_index(drop=True)

    return df


# === WCZYTANIE I PRZYGOTOWANIE METAR ===


def extract_airport_icao_from_metar(raw_metar):

    if pd.isna(raw_metar):
        return None

    text = str(raw_metar).strip().upper()
    match = re.search(r"\b(?:METAR|SPECI)\s+([A-Z]{4})\b", text)

    if match:
        return match.group(1)

    return None


def parse_wind(wind_text):

    result = {
        "wind_speed_kt": np.nan,
        "wind_gust_kt": np.nan
    }

    if pd.isna(wind_text):
        return result

    text = str(wind_text).strip().upper()

    match = re.search(r"(\d{3}|VRB)(\d{2,3})(G(\d{2,3}))?KT", text)

    if match:
        result["wind_speed_kt"] = float(match.group(2))
        if match.group(4):
            result["wind_gust_kt"] = float(match.group(4))

    return result


def parse_cloud_ceiling_ft(cloud_text):

    if pd.isna(cloud_text):
        return np.nan

    text = str(cloud_text).strip().upper()
    heights = []

    for layer in re.findall(r"\b(BKN|OVC|VV)(\d{3})\b", text):
        layer_type, height_hundreds = layer
        height_ft = int(height_hundreds) * 100
        heights.append(height_ft)

    if not heights:
        return np.nan

    return min(heights)


def extract_weather_codes(weather_text, raw_metar):

    codes = []

    if pd.notna(weather_text):
        wt = str(weather_text).strip().upper()
        if wt:
            codes.append(wt)

    if pd.notna(raw_metar):
        raw = str(raw_metar).upper()

        important_patterns = [
            "+TSRA", "+SN", "+FZRA",
            "TSRA", "TS", "FG", "BR", "RA", "DZ", "SN",
            "FZRA", "FZDZ", "SHSN", "SHRA", "BLSN", "SQ", "HZ", "FU"
        ]

        for p in important_patterns:
            if p in raw and p not in codes:
                codes.append(p)

    return list(dict.fromkeys(codes))


def load_and_prepare_metar(filepath):
    df = pd.read_excel(filepath)

    df["utc_time"] = df["utc_time"].astype(str).str.strip()

    # datetime
    df["metar_datetime_utc"] = parse_datetime(df["utc_date"], df["utc_time"])

    # ICAO z METAR raw
    df["airport_icao"] = df["METAR (raw)"].apply(extract_airport_icao_from_metar)

    # parsowanie wiatru
    wind_parsed = df["Wiatr"].apply(parse_wind)
    df["wind_speed_kt"] = wind_parsed.apply(lambda x: x["wind_speed_kt"])
    df["wind_gust_kt"] = wind_parsed.apply(lambda x: x["wind_gust_kt"])

    # ceiling
    df["ceiling_ft"] = df["Chmury"].apply(parse_cloud_ceiling_ft)

    # kody pogody
    df["weather_codes"] = df.apply(
        lambda row: extract_weather_codes(row.get("Pogoda"), row.get("METAR (raw)")),
        axis=1
    )

    df = df.dropna(subset=["metar_datetime_utc", "airport_icao"])
    df["airport_icao"] = df["airport_icao"].astype(str).str.strip().str.upper()

    df = df.sort_values(["airport_icao", "metar_datetime_utc"]).reset_index(drop=True)

    return df


# === REGUŁY EKSPERCKIE ===

def score_visibility(visibility_m, reasons):
    if pd.isna(visibility_m):
        return 0

    visibility_m = safe_int(visibility_m, 9999)

    if visibility_m >= 5000:
        return 0
    elif 3000 <= visibility_m <= 4999:
        reasons.append(f"widzialnosc {visibility_m} m")
        return 1
    elif 1500 <= visibility_m <= 2999:
        reasons.append(f"obnizona widzialnosc {visibility_m} m")
        return 2
    else:
        reasons.append(f"bardzo niska widzialnosc {visibility_m} m")
        return 3


def score_weather(weather_codes, visibility_m, reasons):
    if not weather_codes:
        return 0

    codes_set = set([str(x).upper() for x in weather_codes])

    if "+TSRA" in codes_set or "+SN" in codes_set or "+FZRA" in codes_set:
        reasons.append(f"silne zjawiska pogodowe: {', '.join(sorted(codes_set))}")
        return 3

    if "FG" in codes_set and pd.notna(visibility_m) and safe_int(visibility_m, 9999) < 1000:
        reasons.append("mgla FG przy widzialnosci < 1000 m")
        return 3

    strong = {"FG", "FZRA", "FZDZ", "TS", "TSRA", "SHSN", "SHRA", "BLSN", "SQ"}
    moderate = {"RA", "DZ", "SN", "BR", "HZ", "FU"}

    if codes_set & strong:
        reasons.append(f"niekorzystne zjawiska: {', '.join(sorted(codes_set & strong))}")
        return 2

    if codes_set & moderate:
        reasons.append(f"zjawiska pogodowe: {', '.join(sorted(codes_set & moderate))}")
        return 1

    return 0


def score_clouds(ceiling_ft, reasons):
    if pd.isna(ceiling_ft):
        return 0

    ceiling_ft = safe_int(ceiling_ft, 99999)

    if ceiling_ft >= 1000:
        return 0
    elif 500 <= ceiling_ft <= 999:
        reasons.append(f"niska podstawa chmur {ceiling_ft} ft")
        return 1
    elif 300 <= ceiling_ft <= 499:
        reasons.append(f"bardzo niska podstawa chmur {ceiling_ft} ft")
        return 2
    else:
        reasons.append(f"krytycznie niska podstawa chmur {ceiling_ft} ft")
        return 3


def score_wind(wind_speed_kt, wind_gust_kt, reasons):
    wind_score = 0
    gust_score = 0

    if pd.notna(wind_speed_kt):
        speed = float(wind_speed_kt)

        if speed < 15:
            wind_score = 0
        elif speed <= 24:
            wind_score = 1
            reasons.append(f"wiatr {int(speed)} kt")
        elif speed <= 34:
            wind_score = 2
            reasons.append(f"silny wiatr {int(speed)} kt")
        else:
            wind_score = 3
            reasons.append(f"bardzo silny wiatr {int(speed)} kt")

    if pd.notna(wind_gust_kt):
        gust = float(wind_gust_kt)

        if gust < 25:
            gust_score = 0
        elif gust <= 34:
            gust_score = 1
            reasons.append(f"porywy {int(gust)} kt")
        elif gust <= 44:
            gust_score = 2
            reasons.append(f"silne porywy {int(gust)} kt")
        else:
            gust_score = 3
            reasons.append(f"bardzo silne porywy {int(gust)} kt")

    return max(wind_score, gust_score)


def score_qnh(qnh_hpa, reasons):
    if pd.isna(qnh_hpa):
        return 0

    qnh_hpa = safe_int(qnh_hpa, 1013)

    if qnh_hpa >= 990:
        return 0
    elif 980 <= qnh_hpa <= 989:
        reasons.append(f"obnizone cisnienie {qnh_hpa} hPa")
        return 1
    else:
        reasons.append(f"bardzo niskie cisnienie {qnh_hpa} hPa")
        return 2


def score_combined(visibility_m, weather_codes, ceiling_ft, wind_speed_kt, reasons):
    vis = safe_int(visibility_m, 9999) if pd.notna(visibility_m) else 9999
    ceil = safe_int(ceiling_ft, 99999) if pd.notna(ceiling_ft) else 99999
    wind = float(wind_speed_kt) if pd.notna(wind_speed_kt) else 0.0
    codes = set([str(x).upper() for x in weather_codes]) if weather_codes else set()

    combined_score = 0
    combined_reasons = []

    if vis < 1500 and ("FG" in codes or "BR" in codes):
        combined_score = max(combined_score, 2)
        combined_reasons.append("polaczenie niskiej widzialnosci i mgly/zamglenia")

    if vis < 3000 and ceil < 500:
        combined_score = max(combined_score, 2)
        combined_reasons.append("niska widzialnosc i niski ceiling")

    if "TS" in codes or "TSRA" in codes:
        combined_score = max(combined_score, 2)
        combined_reasons.append("burza w rejonie lotniska")

    if "FZRA" in codes or "FZDZ" in codes:
        combined_score = max(combined_score, 2)
        combined_reasons.append("marznace opady")

    if "SN" in codes and vis < 3000:
        combined_score = max(combined_score, 2)
        combined_reasons.append("snieg i ograniczona widzialnosc")

    if wind >= 25 and vis < 3000:
        combined_score = max(combined_score, 2)
        combined_reasons.append("silny wiatr i ograniczona widzialnosc")

    reasons.extend(combined_reasons)

    return combined_score


def force_high_risk(visibility_m, weather_codes, ceiling_ft, wind_speed_kt, wind_gust_kt):
    vis = safe_int(visibility_m, 9999) if pd.notna(visibility_m) else 9999
    ceil = safe_int(ceiling_ft, 99999) if pd.notna(ceiling_ft) else 99999
    wind = float(wind_speed_kt) if pd.notna(wind_speed_kt) else 0.0
    gust = float(wind_gust_kt) if pd.notna(wind_gust_kt) else 0.0
    codes = set([str(x).upper() for x in weather_codes]) if weather_codes else set()

    if "TS" in codes or "TSRA" in codes:
        return True
    if "FZRA" in codes or "FZDZ" in codes:
        return True
    if "FG" in codes and vis < 1000:
        return True
    if ceil < 200:
        return True
    if wind >= 45 or gust >= 45:
        return True

    return False


def evaluate_metar_risk(metar_row):
    if metar_row is None:
        return {
            "score": 0,
            "level": "BRAK RYZYKA",
            "reason": "Brak danych METAR",
            "raw": None,
            "metar_time": None
        }

    reasons = []

    visibility_m = metar_row.get("Widzialnosc (m)")
    weather_codes = metar_row.get("weather_codes", [])
    ceiling_ft = metar_row.get("ceiling_ft")
    wind_speed_kt = metar_row.get("wind_speed_kt")
    wind_gust_kt = metar_row.get("wind_gust_kt")
    qnh_hpa = metar_row.get("QNH (hPa)")

    score = 0
    score += score_visibility(visibility_m, reasons)
    score += score_weather(weather_codes, visibility_m, reasons)
    score += score_clouds(ceiling_ft, reasons)
    score += score_wind(wind_speed_kt, wind_gust_kt, reasons)
    score += score_qnh(qnh_hpa, reasons)
    score += score_combined(visibility_m, weather_codes, ceiling_ft, wind_speed_kt, reasons)

    level = level_from_score(score)

    if force_high_risk(visibility_m, weather_codes, ceiling_ft, wind_speed_kt, wind_gust_kt):
        level = "WYSOKIE RYZYKO"
        if score < 9:
            score = 9

    if not reasons:
        reasons = ["Brak istotnych czynnikow pogodowych"]

    return {
        "score": int(score),
        "level": level,
        "reason": "; ".join(dict.fromkeys(reasons)),
        "raw": metar_row.get("METAR (raw)"),
        "metar_time": metar_row.get("metar_datetime_utc")
    }


# === DOPASOWANIE METAR ===

def build_metar_dict(metar_df):

    metar_dict = {}

    for airport, group in metar_df.groupby("airport_icao"):
        metar_dict[airport] = (
            group
            .sort_values("metar_datetime_utc")
            .reset_index(drop=True)
        )

    return metar_dict


def find_latest_metar(metar_dict, airport_icao, flight_time):
    airport_icao = normalize_text(airport_icao)

    if airport_icao not in metar_dict:
        return None

    airport_metars = metar_dict[airport_icao]
    valid = airport_metars[airport_metars["metar_datetime_utc"] <= flight_time]

    if valid.empty:
        return None

    return valid.iloc[-1].to_dict()


def get_airports_to_analyze(departure_airport_icao, arrival_airport_icao):
    """
    ABROAD -> ABROAD: brak analizy METAR

    POLSKA -> ABROAD: tylko lotnisko odlotu

    ABROAD -> POLSKA: tylko lotnisko przylotu

    POLSKA -> POLSKA: oba lotniska
    """
    dep = normalize_text(departure_airport_icao)
    arr = normalize_text(arrival_airport_icao)

    dep_is_polish = is_polish_airport_code(dep)
    arr_is_polish = is_polish_airport_code(arr)

    if not dep_is_polish and not arr_is_polish:
        return {
            "analyze_departure": False,
            "analyze_arrival": False,
            "flight_scope": "OVERFLIGHT_OR_ABROAD"
        }

    if dep_is_polish and not arr_is_polish:
        return {
            "analyze_departure": True,
            "analyze_arrival": False,
            "flight_scope": "OUTBOUND_FROM_POLAND"
        }

    if not dep_is_polish and arr_is_polish:
        return {
            "analyze_departure": False,
            "analyze_arrival": True,
            "flight_scope": "INBOUND_TO_POLAND"
        }

    return {
        "analyze_departure": True,
        "analyze_arrival": True,
        "flight_scope": "DOMESTIC_OR_POLAND_ONLY"
    }


# === ŁĄCZENIE RYZYKA ODLOT/PRZYLOT ===

def combine_risks(dep_result, arr_result, analyze_departure=True, analyze_arrival=True):
    dep_score = dep_result["score"]
    arr_score = arr_result["score"]

    final_score = max(dep_score, arr_score)
    final_level = level_from_score(final_score)

    if dep_result["level"] == "WYSOKIE RYZYKO" or arr_result["level"] == "WYSOKIE RYZYKO":
        final_level = "WYSOKIE RYZYKO"
        final_score = max(final_score, 9)

    if not analyze_departure and not analyze_arrival:
        main_source = "NONE"
        reason = "Lot przelotowy lub poza zakresem polskich lotnisk — nie analizowano METAR"
    elif analyze_departure and not analyze_arrival:
        main_source = "DEPARTURE"
        reason = f"Analizowano METAR lotniska odlotu: {dep_result['reason']}"
    elif not analyze_departure and analyze_arrival:
        main_source = "ARRIVAL"
        reason = f"Analizowano METAR lotniska przylotu: {arr_result['reason']}"
    else:
        if dep_score == 0 and arr_score == 0:
            main_source = "NONE"
            reason = "Brak istotnych czynnikow pogodowych na lotnisku odlotu i przylotu"
        elif dep_score > arr_score:
            main_source = "DEPARTURE"
            reason = f"Wyzsze ryzyko na odlocie: {dep_result['reason']}"
        elif arr_score > dep_score:
            main_source = "ARRIVAL"
            reason = f"Wyzsze ryzyko na przylocie: {arr_result['reason']}"
        else:
            main_source = "BOTH"
            reason = (
                f"Porownywalne ryzyko na obu lotniskach | "
                f"ODLOT: {dep_result['reason']} | "
                f"PRZYLOT: {arr_result['reason']}"
            )

    return {
        "final_score": int(final_score),
        "final_level": final_level,
        "main_source": main_source,
        "reason": reason
    }


# === JEDNA MINUTA ANALIZY ===

def process_minute(minute_df, current_simulation_time, metar_dict, minute_index):
    if minute_df.empty:
        return pd.DataFrame(columns=[
            "flight_id", "simulation_time_utc", "flight_datetime_utc", "utc_date", "utc_time",
            "callsign", "icao24", "lat", "lon", "geo_altitude", "baro_altitude",
            "velocity", "vertical_rate", "true_track",
            "departure_airport_icao", "arrival_airport_icao", "flight_scope",
            "dep_metar_time", "dep_metar_raw",
            "arr_metar_time", "arr_metar_raw",
            "dep_risk_score", "dep_risk_level", "dep_risk_reason",
            "arr_risk_score", "arr_risk_level", "arr_risk_reason",
            "final_risk_score", "risk_level", "risk_reason", "main_risk_source",
            "is_active", "record_count_in_minute", "analysis_status",
            "minute_index", "run_id", "export_timestamp"
        ])

    # liczba rekordow na callsign w tej minucie
    counts = (
        minute_df
        .groupby("callsign")
        .size()
        .rename("record_count_in_minute")
        .reset_index()
    )

    # ostatni rekord w minucie dla kazdego callsign
    active_flights = (
        minute_df
        .sort_values("flight_datetime_utc")
        .groupby("callsign", as_index=False)
        .tail(1)
        .copy()
    )

    active_flights = active_flights.merge(counts, on="callsign", how="left")

    results = []

    for _, flight in active_flights.iterrows():
        dep_airport = normalize_text(flight["departure_airport_icao"])
        arr_airport = normalize_text(flight["arrival_airport_icao"])

        analysis_plan = get_airports_to_analyze(dep_airport, arr_airport)

        analyze_departure = analysis_plan["analyze_departure"]
        analyze_arrival = analysis_plan["analyze_arrival"]
        flight_scope = analysis_plan["flight_scope"]

        dep_metar = None
        arr_metar = None

        if analyze_departure:
            dep_metar = find_latest_metar(
                metar_dict,
                dep_airport,
                flight["flight_datetime_utc"]
            )

        if analyze_arrival:
            arr_metar = find_latest_metar(
                metar_dict,
                arr_airport,
                flight["flight_datetime_utc"]
            )

        if not analyze_departure and not analyze_arrival:
            analysis_status = "OVERFLIGHT_NO_METAR_ANALYSIS"

        elif analyze_departure and not analyze_arrival:
            if dep_metar is None:
                analysis_status = "MISSING_DEP_METAR"
            else:
                analysis_status = "OK_DEP_ONLY"

        elif not analyze_departure and analyze_arrival:
            if arr_metar is None:
                analysis_status = "MISSING_ARR_METAR"
            else:
                analysis_status = "OK_ARR_ONLY"

        else:
            if dep_metar is None and arr_metar is None:
                analysis_status = "NO_METAR_AVAILABLE"
            elif dep_metar is None:
                analysis_status = "MISSING_DEP_METAR"
            elif arr_metar is None:
                analysis_status = "MISSING_ARR_METAR"
            else:
                analysis_status = "OK_BOTH"

        if analyze_departure:
            dep_result = evaluate_metar_risk(dep_metar)
        else:
            dep_result = no_risk_result("Lotnisko odlotu poza Polska — METAR odlotu nieanalizowany")

        if analyze_arrival:
            arr_result = evaluate_metar_risk(arr_metar)
        else:
            arr_result = no_risk_result("Lotnisko przylotu poza Polska — METAR przylotu nieanalizowany")

        final_result = combine_risks(
            dep_result=dep_result,
            arr_result=arr_result,
            analyze_departure=analyze_departure,
            analyze_arrival=analyze_arrival
        )

        flight_id = f"{flight['callsign']}_{current_simulation_time.strftime('%Y%m%d_%H%M')}"

        result_row = {
            "flight_id": flight_id,
            "simulation_time_utc": current_simulation_time,
            "flight_datetime_utc": flight["flight_datetime_utc"],
            "utc_date": str(flight["utc_date"]),
            "utc_time": str(flight["utc_time"]),
            "callsign": flight["callsign"],
            "icao24": flight.get("icao24"),
            "lat": flight.get("lat"),
            "lon": flight.get("lon"),
            "geo_altitude": flight.get("geoaltitude"),
            "baro_altitude": flight.get("baroaltitude"),
            "velocity": flight.get("velocity"),
            "vertical_rate": flight.get("vertrate"),
            "true_track": flight.get("heading"),
            "departure_airport_icao": dep_airport,
            "arrival_airport_icao": arr_airport,
            "flight_scope": flight_scope,
            "dep_metar_time": dep_result["metar_time"],
            "dep_metar_raw": dep_result["raw"],
            "arr_metar_time": arr_result["metar_time"],
            "arr_metar_raw": arr_result["raw"],
            "dep_risk_score": dep_result["score"],
            "dep_risk_level": dep_result["level"],
            "dep_risk_reason": dep_result["reason"],
            "arr_risk_score": arr_result["score"],
            "arr_risk_level": arr_result["level"],
            "arr_risk_reason": arr_result["reason"],
            "final_risk_score": final_result["final_score"],
            "risk_level": final_result["final_level"],
            "risk_reason": final_result["reason"],
            "main_risk_source": final_result["main_source"],
            "is_active": 1,
            "record_count_in_minute": int(flight["record_count_in_minute"]),
            "analysis_status": analysis_status,
            "minute_index": minute_index,
            "run_id": RUN_ID,
            "export_timestamp": pd.Timestamp.utcnow()
        }

        results.append(result_row)

    return pd.DataFrame(results)


# === ZAPIS ===

def save_live_file(df):
    live_df = df.drop(columns=["minute_index", "run_id", "export_timestamp"], errors="ignore")
    live_df.to_csv(LIVE_OUTPUT_FILE, index=False, encoding="utf-8-sig")


def append_history_file(df):
    file_exists = os.path.exists(HISTORY_OUTPUT_FILE)

    df.to_csv(
        HISTORY_OUTPUT_FILE,
        mode="a",
        header=not file_exists,
        index=False,
        encoding="utf-8-sig"
    )


# === SYMULACJA ===

def run_simulation_test_mode(flights_df, metar_df):
    ensure_output_dir()

    if os.path.exists(HISTORY_OUTPUT_FILE):
        os.remove(HISTORY_OUTPUT_FILE)

    metar_dict = build_metar_dict(metar_df)

    simulation_start_time = flights_df["minute_bucket"].min()
    simulation_end_time = flights_df["minute_bucket"].max()

    current_simulation_time = simulation_start_time
    minute_index = 1
    processed_minutes = 0

    print("=" * 70)
    print("START SYMULACJI")
    print(f"Poczatek: {simulation_start_time}")
    print(f"Koniec:   {simulation_end_time}")
    print("=" * 70)

    while current_simulation_time <= simulation_end_time:
        minute_df = flights_df[flights_df["minute_bucket"] == current_simulation_time].copy()

        results_df = process_minute(
            minute_df=minute_df,
            current_simulation_time=current_simulation_time,
            metar_dict=metar_dict,
            minute_index=minute_index
        )

        save_live_file(results_df)

        if not results_df.empty:
            append_history_file(results_df)

        total_active = len(results_df)
        high_risk = 0 if results_df.empty else (results_df["risk_level"] == "WYSOKIE RYZYKO").sum()
        elevated = 0 if results_df.empty else (results_df["risk_level"] == "PODWYZSZONE RYZYKO").sum()
        overflights = 0 if results_df.empty else (results_df["analysis_status"] == "OVERFLIGHT_NO_METAR_ANALYSIS").sum()

        print(
            f"[{minute_index:04d}] {current_simulation_time} | "
            f"aktywnych lotów: {total_active} | "
            f"przelotowe bez METAR: {overflights} | "
            f"podwyższone: {elevated} | wysokie: {high_risk}"
        )

        current_simulation_time += timedelta(minutes=1)
        minute_index += 1
        processed_minutes += 1

        if MAX_MINUTES_TO_PROCESS is not None and processed_minutes >= MAX_MINUTES_TO_PROCESS:
            print(f"Przerwano po {processed_minutes} minutach (MAX_MINUTES_TO_PROCESS).")
            break

        if REALTIME_MODE:
            import time
            time.sleep(SLEEP_SECONDS)

    print("=" * 70)
    print("KONIEC SYMULACJI")
    print(f"Plik live:    {LIVE_OUTPUT_FILE}")
    print(f"Plik history: {HISTORY_OUTPUT_FILE}")
    print("=" * 70)


# === MAIN ===

def main():
    print("Wczytywanie lotow...")
    flights_df = load_and_prepare_flights(FLIGHTS_FILE)

    print("Wczytywanie METAR...")
    metar_df = load_and_prepare_metar(METAR_FILE)

    print(f"Liczba rekordow lotow po czyszczeniu: {len(flights_df)}")
    print(f"Liczba rekordow METAR po czyszczeniu: {len(metar_df)}")

    print()
    print("Kontrola typow lotow po lotniskach:")
    print(
        flights_df[["callsign", "departure_airport_icao", "arrival_airport_icao"]]
        .drop_duplicates()
        .head(10)
    )

    run_simulation_test_mode(flights_df, metar_df)


if __name__ == "__main__":
    main()