import re
import json
import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Optional, Set, List, Tuple

import pandas as pd
from shapely.geometry import shape, Point
from shapely.ops import unary_union

# === KONFIGURACJA ===
POLAND_GEOJSON_NAME = "poland.geojson"
SUPPORTED_EXTENSIONS = {".csv", ".json", ".xlsx", ".xls"}


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
        return next((cols[c.lower()] for c in cands if c.lower() in cols), None)

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


def read_first_row(path: Path) -> pd.DataFrame:
    """
    Odczytuje pierwszy rekord z pliku.
    Służy do automatycznego pobrania daty z kolumny utc_date.
    """
    ext = path.suffix.lower()

    if ext == ".csv":
        return pd.read_csv(path, nrows=1)

    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, nrows=1)

    if ext == ".json":
        df = read_raw_file(path)
        return df.head(1)

    raise ValueError(f"Nieobslugiwane rozszerzenie: {ext}")


def get_day_str_from_first_file(first_file: Path) -> str:
    """
    Pobiera datę z kolumny utc_date z pierwszego rekordu pierwszego pliku danych.
    """
    df = read_first_row(first_file)

    if df.empty:
        raise ValueError(f"Plik {first_file.name} nie zawiera żadnych rekordów.")

    cols = {c.lower(): c for c in df.columns}

    if "utc_date" not in cols:
        raise ValueError(
            f"Nie znaleziono kolumny 'utc_date' w pierwszym pliku: {first_file.name}"
        )

    utc_date_col = cols["utc_date"]
    value = df.iloc[0][utc_date_col]

    if pd.isna(value):
        raise ValueError(
            f"Pierwszy rekord w pliku {first_file.name} ma pustą wartość w kolumnie 'utc_date'."
        )

    if hasattr(value, "strftime"):
        day_str = value.strftime("%Y-%m-%d")
    else:
        day_str = str(value).strip()[:10]

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_str):
        raise ValueError(
            f"Nieprawidłowy format daty w kolumnie 'utc_date': {value}. "
            "Oczekiwany format to YYYY-MM-DD."
        )

    return day_str


def find_all_state_data_files(data_dir: Path) -> List[Path]:
    """
    Wyszukuje wszystkie pliki danych typu states_YYYY-MM-DD-HH
    z obsługiwanymi rozszerzeniami.
    """
    pattern = re.compile(
        r"states_\d{4}-\d{2}-\d{2}-\d{2}\.(csv|json|xlsx|xls)$",
        re.IGNORECASE
    )

    files = [
        f for f in data_dir.iterdir()
        if f.is_file()
           and f.suffix.lower() in SUPPORTED_EXTENSIONS
           and pattern.fullmatch(f.name)
    ]

    return sorted(files, key=lambda x: x.name)


def filter_files_for_day(files: List[Path], day_str: str) -> List[Path]:
    """
    Zostawia tylko pliki dotyczące dnia wykrytego z kolumny utc_date.
    """
    pattern = re.compile(
        rf"states_{re.escape(day_str)}-\d{{2}}\.(csv|json|xlsx|xls)$",
        re.IGNORECASE
    )

    return sorted(
        [f for f in files if pattern.fullmatch(f.name)],
        key=lambda x: x.name
    )


# === ANALIZA DNIA ===
def analyze_day(files: List[Path], pl_geom, output_excel: str):
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

        hour_tag = re.search(
            r"states_\d{4}-\d{2}-\d{2}-(\d{2})",
            path.stem,
            re.IGNORECASE
        )

        hour = hour_tag.group(1) if hour_tag else "??"

        raw_df = read_raw_file(path)

        df = normalize_df(raw_df)
        df = df.sort_values("ts")

        df["inside"] = df.apply(
            lambda r: inside_pl(pl_geom, r["lat"], r["lon"]),
            axis=1
        )

        inside_idx = df.index[df["inside"]]
        if len(inside_idx) > 0:
            inside_raw = raw_df.loc[inside_idx].copy()
            inside_raw["hour"] = hour
            inside_raw["source_file"] = path.name
            collected_dfs.append(inside_raw)

            combined = pd.concat(collected_dfs, ignore_index=True)
            combined.to_excel(output_excel, index=False)

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
        day_dep |= dep_set
        day_arr |= arr_set

    return day_over, day_dep, day_arr, per_hour_over, per_hour_dep, per_hour_arr


def get_script_directory() -> Path:
    return Path(__file__).resolve().parent


def find_default_poland_geojson(script_dir: Path) -> Path:
    """
    Szuka pliku poland.geojson w domyślnych lokalizacjach:
    1. w folderze skryptu,
    2. w folderze res znajdującym się obok skryptu.
    """
    possible_paths = [
        script_dir / POLAND_GEOJSON_NAME,
        script_dir / "res" / POLAND_GEOJSON_NAME,
        ]

    for path in possible_paths:
        if path.exists():
            return path.resolve()

    checked_paths = "\n".join(f"- {path}" for path in possible_paths)

    raise SystemExit(
        "Nie podano ścieżki do pliku poland.geojson i nie znaleziono go "
        "w domyślnych lokalizacjach:\n"
        f"{checked_paths}"
    )


def get_paths_from_args() -> Tuple[Path, Path]:
    script_dir = get_script_directory()

    parser = argparse.ArgumentParser(
        description="Analizuje loty nad Polską na podstawie plików CSV/JSON/XLSX."
    )

    parser.add_argument(
        "data_dir",
        nargs="?",
        default=str(script_dir),
        help=(
            "Ścieżka do folderu z danymi. "
            "Jeśli nie zostanie podana, użyty zostanie folder skryptu."
        )
    )

    parser.add_argument(
        "poland_geojson",
        nargs="?",
        default=None,
        help=(
            "Ścieżka do pliku poland.geojson. "
            "Jeśli nie zostanie podana, skrypt szuka pliku w folderze skryptu "
            "oraz w folderze res obok skryptu."
        )
    )

    args = parser.parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve()

    if args.poland_geojson:
        poland_geojson = Path(args.poland_geojson).expanduser().resolve()
    else:
        poland_geojson = find_default_poland_geojson(script_dir)

    return data_dir, poland_geojson


# === MAIN ===
def main():
    data_dir, poland_geojson = get_paths_from_args()

    print(f"Wykorzystywana ścieżka danych: {data_dir}")
    print(f"Wykorzystywany plik GeoJSON: {poland_geojson}")

    if not data_dir.is_dir():
        raise SystemExit(f"Nie znaleziono folderu z danymi: {data_dir}")

    if not poland_geojson.exists():
        raise SystemExit(f"Nie znaleziono pliku GeoJSON: {poland_geojson}")

    all_files = find_all_state_data_files(data_dir)

    if not all_files:
        raise SystemExit(
            f"Nie znaleziono plików danych typu states_YYYY-MM-DD-HH w folderze: {data_dir}"
        )

    first_file = all_files[0]
    day_str = get_day_str_from_first_file(first_file)

    print(f"Pierwszy analizowany plik: {first_file.name}")
    print(f"Wykryta data z kolumny utc_date: {day_str}")

    files = filter_files_for_day(all_files, day_str)

    if not files:
        raise SystemExit(f"Nie znaleziono plikow dla dnia {day_str} w {data_dir}")

    print(f"Znaleziono {len(files)} plików dla dnia {day_str}.")

    output_txt = f"poland_flights_summary_{day_str}.txt"
    output_excel = f"flights_in_poland_{day_str}.xlsx"

    pl_geom = load_poland_geometry(str(poland_geojson))

    day_over, day_dep, day_arr, perh_over, perh_dep, perh_arr = analyze_day(
        files,
        pl_geom,
        output_excel
    )

    # === RAPORT ===
    lines = []
    lines.append(f"Podsumowanie dla dnia {day_str}")
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

    with open(output_txt, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\nZapisano raport do pliku: {output_txt}")
    print(f"Zebrane rekordy z terenu Polski sa w pliku: {output_excel}")


if __name__ == "__main__":
    main()