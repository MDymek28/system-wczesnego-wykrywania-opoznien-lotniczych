import os
import re
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Optional, Set, List, Tuple

import pandas as pd
from shapely.geometry import shape, Point
from shapely.ops import unary_union

# === KONFIGURACJA ===
# INPUT
DATA_DIR = "/Users/malgorzata/Library/CloudStorage/OneDrive-SGH/PRACA MAGISTERSKA/2022_06_27/DANE"
DAY_STR  = "2022-06-27"
POLAND_GEOJSON = "/Users/malgorzata/Library/CloudStorage/OneDrive-SGH/PRACA MAGISTERSKA/2022_06_27/DANE/poland.geojson"

# OUTPUT
OUTPUT_TXT   = f"poland_flights_summary_{DAY_STR}.txt"
OUTPUT_EXCEL = f"flights_in_poland_{DAY_STR}.xlsx"
# ==================================================

@dataclass
class AState:
    last_inside: Optional[bool] = None
    last_on_ground: Optional[bool] = None

# === GEO ===
def load_poland_geometry(path: str):
    with open(path, "r", encoding="utf-8") as f:
        gj = json.load(f)
    geoms = []
    t = gj.get("type")
    if t == "FeatureCollection":
        for feat in gj["features"]:
            geoms.append(shape(feat["geometry"]))
    elif t in ("Polygon", "MultiPolygon", "GeometryCollection"):
        geoms.append(shape(gj))
    elif t == "Feature":
        geoms.append(shape(gj["geometry"]))
    else:
        raise ValueError("Nieznany format GeoJSON.")
    return geoms[0] if len(geoms) == 1 else unary_union(geoms)


def inside_pl(poly, lat, lon) -> bool:
    if pd.isna(lat) or pd.isna(lon):
        return False
    pt = Point(float(lon), float(lat))
    return poly.contains(pt) or poly.touches(pt)

# === WYBIERANIE ODPOWIEDNICH LOTOW ===
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}

    def has(*cands):
        return next((cols[c] for c in cands if c in cols), None)

    cs = has("callsign")
    if cs is None:
        raise ValueError("Wymagana kolumna 'callsign'.")

    lat = has("lat", "latitude", "y")
    lon = has("lon", "longitude", "lng", "x")
    if not lat or not lon:
        raise ValueError("Brakuje wspolrzednych (lat/lon).")

    ts = has("last_contact", "time_position", "timestamp", "time", "seen")
    og = has("on_ground", "onground", "onGround")

    out = pd.DataFrame({
        "callsign": df[cs].astype(str).str.strip(),
        "lat": df[lat],
        "lon": df[lon],
        "on_ground": df[og] if og else False,
        "ts": df[ts] if ts else range(len(df)),
    }, index=df.index)   # zachowujemy index zgodny z oryginałem

    # PUSTY CALLSIGN -> OUT
    mask_cs = (out["callsign"].notna()) & (out["callsign"] != "")
    out = out[mask_cs]

    # on_ground -> bool
    out["on_ground"] = out["on_ground"].apply(
        lambda v: str(v).strip().lower() in ("1", "true", "t", "yes", "y")
        if pd.notna(v) else False
    )
    return out


def read_raw_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    elif ext == ".csv":
        return pd.read_csv(path)
    elif ext == ".json":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict) and "states" in data:
            rows = []
            for s in data.get("states") or []:
                try:
                    rows.append({
                        "callsign": s[1],
                        "lat": s[6],
                        "lon": s[5],
                        "on_ground": s[8],
                        "ts": s[4],
                    })
                except Exception:
                    continue
            return pd.DataFrame(rows)
        if isinstance(data, list):
            return pd.DataFrame(data)
        for v in (data or {}).values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return pd.DataFrame(v)
        raise ValueError(f"Nieznany uklad JSON w {path}")
    else:
        raise ValueError(f"Nieobslugiwane rozszerzenie: {ext}")


# === ANALIZA DNIA ===
def analyze_day(files: List[Path], pl_geom):
    day_over: Set[str] = set()
    day_dep: Set[str] = set()
    day_arr: Set[str] = set()

    per_hour_over: Dict[str, Set[str]] = {}
    per_hour_dep: Dict[str, Set[str]] = {}
    per_hour_arr: Dict[str, Set[str]] = {}

    state: Dict[str, AState] = {}

    collected_dfs: List[pd.DataFrame] = []

    for path in files:
        print(f"Analiza {path.name}...")

        hour_tag = re.search(r"states_\d{4}-\d{2}-\d{2}-(\d{2})", path.stem, re.IGNORECASE)
        hour = hour_tag.group(1) if hour_tag else "??"

        raw_df = read_raw_file(path)

        df = normalize_df(raw_df)
        df = df.sort_values("ts")

        df["inside"] = df.apply(lambda r: inside_pl(pl_geom, r["lat"], r["lon"]), axis=1)

        inside_idx = df.index[df["inside"]]
        if len(inside_idx) > 0:
            inside_raw = raw_df.loc[inside_idx].copy()
            inside_raw["hour"] = hour
            inside_raw["source_file"] = path.name
            collected_dfs.append(inside_raw)

            combined = pd.concat(collected_dfs, ignore_index=True)
            combined.to_excel(OUTPUT_EXCEL, index=False)

        over_set: Set[str] = set()
        dep_set: Set[str] = set()
        arr_set: Set[str] = set()

        for cs, g in df.groupby("callsign", sort=False):
            g = g[["inside", "on_ground"]].astype({"on_ground": bool})
            st = state.get(cs, AState())

            prev_inside = st.last_inside
            prev_og = st.last_on_ground

            if (g["inside"] & (~g["on_ground"])).any():
                over_set.add(cs)

            for inside, og in g.itertuples(index=False):
                if prev_inside is not None:
                    # START Z PL
                    if (prev_inside is True and prev_og is True) and (inside is True and og is False):
                        dep_set.add(cs)
                    # LADOWANIE W PL
                    if (prev_og is False) and (inside is True and og is True):
                        arr_set.add(cs)
                prev_inside, prev_og = inside, og

            st.last_inside = prev_inside
            st.last_on_ground = prev_og
            state[cs] = st

        per_hour_over[hour] = over_set
        per_hour_dep[hour] = dep_set
        per_hour_arr[hour] = arr_set

        day_over |= over_set
        day_dep  |= dep_set
        day_arr  |= arr_set

    return day_over, day_dep, day_arr, per_hour_over, per_hour_dep, per_hour_arr


# === MAIN ===
def main():
    pl_geom = load_poland_geometry(POLAND_GEOJSON)

    p = Path(DATA_DIR)
    pattern = re.compile(rf"states_{DAY_STR}-\d{{2}}\..+", re.IGNORECASE)
    files = sorted([f for f in p.iterdir() if pattern.fullmatch(f.name)], key=lambda x: x.name)

    if not files:
        raise SystemExit(f"Nie znaleziono plikow dla dnia {DAY_STR} w {DATA_DIR}")

    day_over, day_dep, day_arr, perh_over, perh_dep, perh_arr = analyze_day(files, pl_geom)

    # === RAPORT ===
    lines = []
    lines.append(f"Podsumowanie dla dnia {DAY_STR}")
    lines.append(f"  Przelecialo nad Polska: {len(day_over)}")
    lines.append(f"  Starty z Polski:        {len(day_dep)}")
    lines.append(f"  Ladowania w Polsce:     {len(day_arr)}")
    lines.append("")
    lines.append("Rozbicie na godziny (HH):")
    for hour in sorted(perh_over.keys()):
        lines.append(
            f"  {hour}: over={len(perh_over[hour])}  dep={len(perh_dep[hour])}  arr={len(perh_arr[hour])}"
        )

    report = "\n".join(lines)

    print("\n" + report)
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\nZapisano raport do pliku: {OUTPUT_TXT}")
    print(f"Zebrane rekordy z terenu Polski a w pliku: {OUTPUT_EXCEL}")

if __name__ == "__main__":
    main()
