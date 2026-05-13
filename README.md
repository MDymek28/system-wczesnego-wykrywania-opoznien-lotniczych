# System wczesnego wykrywania opoznien lotniczych
Opracowanie systemu wczesnego wykrywania i ostrzegania przed opóźnieniami lotniczymi na podstawie analizy strumieni 
danych w czasie rzeczywistym.

## Architektura
System składa się z 5 aplikacji:

### **1_rozpakowywanie_danych.py** — rozpakowanie danych pozyskanych z _OpenSkyNetwork_
Aplikacja służy do automatycznego przetwarzania plików z rozszerzeniem `.json.tar`. 
Po uruchomieniu wybiera folder z danymi, wyszukuje w nim wszystkie pliki `.json.tar`, rozpakowuje je, odczytuje 
znajdujące się w nich pliki `.json.gz`, a następnie wczytuje dane _JSON_. Dla każdego rekordu dodaje kolumny _utc_date_
i _utc_time_ wyliczone na podstawie pola time, a potem zapisuje wynik jako osobny plik _CSV_. Na końcu dla każdego 
przetworzonego pliku wyświetla krótkie podsumowanie oraz podgląd pierwszych pięciu rekordów.

Sposób użycia:

W terminalu należy wywołać aplikację i wskazać ścieżkę do folderu z plikami.
```bash 
python 1_rozpakowywanie_danych.py <ścieżka_do_folderu_z_danymi>
```
- Jeżeli nie wskazano ścieżki, aplikacja wykorzysta lokalizację, w której się znajduje.

### **2_loty_krajowe.py** — wydzielenie lotów krajowych z pozyskanych danych

Skrypt analizuje pliki danych lotniczych _states_YYYY-MM-DD-HH_ znajdujące się we wskazanym folderze i sprawdza, które
rekordy dotyczą obszaru Polski na podstawie pliku `poland.geojson`. Datę analizy wykrywa automatycznie z kolumny _utc_date_
w pierwszym rekordzie pierwszego znalezionego pliku danych. Następnie dla wykrytego dnia zlicza samoloty, które 
przeleciały nad Polską, wystartowały z Polski lub lądowały w Polsce. Wyniki zapisuje do pliku tekstowego z podsumowaniem
oraz do pliku `.xlsx` zawierającego rekordy znajdujące się na terenie Polski.

Sposób użycia:

W terminalu należy wywołać aplikację, wskazać ścieżkę do folderu z wcześniej przygotowanymi danymi, oraz ścieżkę do 
pliku `.geojson` zawierającego informacje o granicach państwa.
```bash
python 2_loty_krajowe.py <ścieżka_do_folderu_z_danymi> <plik_geojson>
```

- Jeżeli nie wskazano ścieżki do pliku `.geojson`, aplikacja spróbuje odnaleźć plik o nazwie `poland.geojson` w folderze,
w którym znajduje się skrypt, oraz w folderze `res`, w lokalizacji skryptu. 

- W przypadku kiedy nie wskazano ścieżki do folderu z danymi, aplikacja spróbuje odnaleźć pliki w folderze, w którym 
jest zlokalizowana.

### **3_tylko_LOT.py** — wydzielenie wyłącznie lotów linii lotniczej LOT

### **4_dopisanie_lotnisk.py** — uzupełnienie danych o lotniska startowe i końcowe

### **5_wykrywanie_opoznien.py** — generowanie informacji o opóźnieniach


Aplikacje 1 - 4 są odpowiedzialne za przygotowanie danych do analizy. 
Aplikacja 5, na podstawie wcześniej przygotowanych danych, generuje wyniki analizy danych. Efektem są 2 pliki:
- _live_risk_map.csv_ — plik z aktualnym przewidywanym stanem opóźnień 
- _risk_history.csv_ — plik agregujący dotychczasowe wyniki analizy



## Wymagania
- Python 3.13+
### Biblioteki:
- Pandas
- Shapely
- Numpy

