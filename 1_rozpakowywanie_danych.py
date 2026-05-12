import tarfile
import gzip
import json
import csv
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

# === ŚCIEŻKA DO FOLDERU Z PLIKAMI .json.tar ===
TAR_DIR = "/Users/malgorzata/Library/CloudStorage/OneDrive-SGH/PRACA MAGISTERSKA/2022_06_27/DANE"

# === ROZPAKOWANIE DANYCH ===
def extract_tar_get_gz(TAR_PATH: str) -> str:
    out_dir = os.path.dirname(os.path.abspath(TAR_PATH)) or "."
    with tarfile.open(TAR_PATH, "r") as tar:
        tar.extractall(out_dir)

    base_name = os.path.splitext(os.path.basename(TAR_PATH))[0]  # np. states_2017-09-18-02
    for name in os.listdir(out_dir):
        if name.startswith(base_name) and name.endswith(".json.gz"):
            return os.path.join(out_dir, name)

    raise FileNotFoundError(f"Nie znaleziono pliku .json.gz odpowiadającego {TAR_PATH}")


def load_json_array_from_gz(gz_path: str) -> List[Dict[str, Any]]:
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Oczekiwano listy obiektów JSON w pliku .json ")
    return data

# === DODAWANIE DATY I CZASU UTC ===
def add_utc_columns(records: List[Dict[str, Any]]) -> None:
    for rec in records:
        ts = rec.get("time")
        if isinstance(ts, (int, float)):
            ts_int = int(ts)
            dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
            rec["utc_date"] = dt.strftime("%Y-%m-%d")
            rec["utc_time"] = dt.strftime("%H:%M:%S")
        else:
            rec["utc_date"] = ""
            rec["utc_time"] = ""


def compute_fieldnames_all(records: List[Dict[str, Any]]) -> List[str]:
    all_keys = set()
    for rec in records:
        all_keys.update(rec.keys())

    preferred_order = []
    if "time" in all_keys:
        preferred_order.append("time")
    if "utc_date" in all_keys:
        preferred_order.append("utc_date")
    if "utc_time" in all_keys:
        preferred_order.append("utc_time")

    remaining = sorted(k for k in all_keys if k not in preferred_order)
    return preferred_order + remaining


def write_csv(records: List[Dict[str, Any]], csv_path: str) -> None:
    fieldnames = compute_fieldnames_all(records)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def process_selected_tar_files(tar_dir: str, tar_files: List[str]) -> None:
    for tar_file in tar_files:
        tar_path = os.path.join(tar_dir, tar_file)

        if not os.path.exists(tar_path):
            print(f"Nie znaleziono pliku: {tar_file}")
            continue

        print(f"Przetwarzam: {tar_file}")

        gz_path = extract_tar_get_gz(tar_path)
        print(f"Rozpakowano .tar. Znaleziono plik: {os.path.basename(gz_path)}")

        records = load_json_array_from_gz(gz_path)
        print(f"Wczytano {len(records):,} rekordów JSON.")

        add_utc_columns(records)

    # === ZAPIS DO CSV ===
        base_json = os.path.splitext(os.path.basename(gz_path))[0]  # np. states_2018-05-28-22.json
        base_no_ext = base_json.replace(".json", "")
        csv_path = os.path.join(os.path.dirname(gz_path), f"{base_no_ext}.csv")

        write_csv(records, csv_path)
        print(f"Zapisano CSV: {csv_path}")

        # === SPRAWDZENIE DANYCH ===
        print("\nPodgląd 5 pierwszych rekordow:")
        preview = records[:5]
        for i, rec in enumerate(preview, 1):
            print(f"\n--- Rekord {i} ---")
            # === SPRAWDZENIE KILKU NAJWAZNIEJSZYCH POL ===
            for k in ["time", "utc_date", "utc_time", "icao24", "callsign", "lat", "lon", "velocity"]:
                if k in rec:
                    print(f"{k}: {rec[k]}")


def main():
    TAR_FILES = [
        "states_2022-06-27-00.json.tar",
        "states_2022-06-27-01.json.tar",
        "states_2022-06-27-02.json.tar",
        "states_2022-06-27-03.json.tar",
        "states_2022-06-27-04.json.tar",
        "states_2022-06-27-05.json.tar",
        "states_2022-06-27-06.json.tar",
        "states_2022-06-27-07.json.tar",
        "states_2022-06-27-08.json.tar",
        "states_2022-06-27-09.json.tar",
        "states_2022-06-27-10.json.tar",
        "states_2022-06-27-11.json.tar",
        "states_2022-06-27-12.json.tar",
        "states_2022-06-27-13.json.tar",
        "states_2022-06-27-14.json.tar",
        "states_2022-06-27-15.json.tar",
        "states_2022-06-27-16.json.tar",
        "states_2022-06-27-17.json.tar",
        "states_2022-06-27-18.json.tar",
        "states_2022-06-27-19.json.tar",
        "states_2022-06-27-20.json.tar",
        "states_2022-06-27-21.json.tar",
        "states_2022-06-27-22.json.tar",
        "states_2022-06-27-23.json.tar",
    ]
    process_selected_tar_files(TAR_DIR, TAR_FILES)


if __name__ == "__main__":
    main()
