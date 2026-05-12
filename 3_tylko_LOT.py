import pandas as pd
from pathlib import Path

# INPUT
input_file = Path("flights_in_poland_2022-06-27.xlsx")

# OUTPUT
output_file = Path("lot_flights_only.xlsx")
df = pd.read_excel(input_file)

if "callsign" not in df.columns:
    raise ValueError("W pliku nie znaleziono kolumny 'callsign'.")

lot_flights = df[df["callsign"].astype(str).str.startswith("LOT", na=False)]
lot_flights.to_excel(output_file, index=False)

print(f"Zapisano {len(lot_flights)} lotow do pliku: {output_file}")