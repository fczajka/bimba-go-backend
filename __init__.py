import os
import time
import requests
import zipfile
import csv
from google.transit import gtfs_realtime_pb2
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict

# Ścieżki do plików
CACHE_DIR = "cache"
GTFS_DIR = os.path.join(CACHE_DIR, "gtfs")
VEHICLE_POSITIONS_FILE = os.path.join(CACHE_DIR, "vehicle_positions.pb")
TRIP_UPDATES_FILE = os.path.join(CACHE_DIR, "trip_updates.pb")
GTFS_ZIP_FILE = os.path.join(CACHE_DIR, "ZTMPoznanGTFS.zip")

# URL-e do pobierania danych
GTFS_ZIP_URL = "https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile"
VEHICLE_POSITIONS_URL = "https://www.ztm.poznan.pl/pl/dla-deweloperow/getGtfsRtFile?file=vehicle_positions.pb"
TRIP_UPDATES_URL = "https://www.ztm.poznan.pl/pl/dla-deweloperow/getGtfsRtFile?file=trip_updates.pb"

# Tworzenie katalogów, jeśli nie istnieją
os.makedirs(GTFS_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True) # Ensure cache directory exists

app = FastAPI(
    title="GTFS Poznań Realtime API",
    description="API do pobierania i przetwarzania danych GTFS-Realtime i statycznych dla Poznania."
)

# Modele Pydantic dla odpowiedzi API
class Position(BaseModel):
    latitude: float
    longitude: float

class VehicleData(BaseModel):
    vehicle_id: str
    route_id: str
    trip_id: str
    position: Position
    delay_seconds: Optional[int] = None
    shape_id: Optional[str] = None
    shape_points: List[Dict[str, float]] = []

class ShapePoint(BaseModel):
    lat: float
    lon: float
    seq: int

# Globalny obiekt do przechowywania przetworzonych danych
class GTFSDataStore:
    def __init__(self):
        self.shapes_dict: Dict[str, List[ShapePoint]] = {}
        self.trips: List[Dict[str, str]] = []
        self.trip_shape_map: Dict[str, str] = {}
        self.trip_delays: Dict[str, int] = {}
        self.vehicle_positions: List[VehicleData] = []
        self.last_updated: Optional[float] = None

data_store = GTFSDataStore()

# Funkcja do pobierania pliku z buforowaniem
def fetch_file(url: str, filepath: str, max_age_seconds: int = 10):
    """
    Pobiera plik z podanego URL-a i zapisuje go w podanej ścieżce.
    Wykorzystuje buforowanie, aby uniknąć ponownego pobierania pliku, jeśli jest świeży.

    Args:
        url (str): URL do pobrania pliku.
        filepath (str): Ścieżka, gdzie plik ma zostać zapisany.
        max_age_seconds (int): Maksymalny wiek pliku w sekundach, zanim zostanie ponownie pobrany.
    """
    if os.path.exists(filepath):
        age = time.time() - os.path.getmtime(filepath)
        if age < max_age_seconds:
            print(f"Plik {filepath} jest świeży (wiek: {age:.2f}s), pomijam pobieranie.")
            return
    print(f"Pobieram plik: {url} do {filepath}...")
    try:
        response = requests.get(url, timeout=30) # Dodano timeout
        response.raise_for_status()  # Sprawdza, czy wystąpił błąd HTTP
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"Pomyślnie pobrano {filepath}.")
    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas pobierania pliku {url}: {e}")
        raise HTTPException(status_code=500, detail=f"Błąd podczas pobierania pliku: {url}")


# Funkcja do wczytywania plików CSV jako listy słowników
def load_csv_dict(filepath: str) -> List[Dict[str, str]]:
    """
    Wczytuje dane z pliku CSV i zwraca je jako listę słowników.
    Obsługuje znak BOM (\ufeff) w nagłówkach kolumn.

    Args:
        filepath (str): Ścieżka do pliku CSV.

    Returns:
        list: Lista słowników, gdzie każdy słownik reprezentuje wiersz danych.
    """
    data = []
    try:
        with open(filepath, mode='r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = [field.replace('\ufeff', '') for field in reader.fieldnames] if reader.fieldnames else []
            reader.fieldnames = fieldnames
            for row in reader:
                cleaned_row = {
                    key.replace('\ufeff', ''): value
                    for key, value in row.items()
                }
                data.append(cleaned_row)
        print(f"Pomyślnie wczytano {len(data)} wierszy z {filepath}.")
    except FileNotFoundError:
        print(f"Błąd: Plik nie znaleziono pod ścieżką {filepath}")
        raise HTTPException(status_code=500, detail=f"Plik CSV nie znaleziono: {filepath}")
    except Exception as e:
        print(f"Błąd podczas wczytywania pliku CSV {filepath}: {e}")
        raise HTTPException(status_code=500, detail=f"Błąd podczas wczytywania pliku CSV: {filepath}")
    return data

# Wczytywanie danych GTFS-Realtime
def load_feed(filepath: str):
    """
    Wczytuje plik GTFS-Realtime i parsuje go do obiektu FeedMessage.

    Args:
        filepath (str): Ścieżka do pliku GTFS-Realtime (.pb).

    Returns:
        gtfs_realtime_pb2.FeedMessage: Sparsowany obiekt FeedMessage.
    """
    try:
        with open(filepath, 'rb') as f:
            data = f.read()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        print(f"Pomyślnie sparsowano plik GTFS-Realtime: {filepath}.")
        return feed
    except FileNotFoundError:
        print(f"Błąd: Plik GTFS-Realtime nie znaleziono pod ścieżką {filepath}")
        raise HTTPException(status_code=500, detail=f"Plik GTFS-Realtime nie znaleziono: {filepath}")
    except Exception as e:
        print(f"Błąd podczas parsowania pliku GTFS-Realtime {filepath}: {e}")
        raise HTTPException(status_code=500, detail=f"Błąd podczas parsowania pliku GTFS-Realtime: {filepath}")


async def fetch_and_process_data():
    """
    Pobiera i przetwarza wszystkie dane GTFS (statyczne i w czasie rzeczywistym)
    oraz aktualizuje globalny magazyn danych.
    """
    print("Rozpoczynam pobieranie i przetwarzanie danych...")
    try:
        # Pobieranie danych GTFS-Realtime
        fetch_file(VEHICLE_POSITIONS_URL, VEHICLE_POSITIONS_FILE, max_age_seconds=10)
        fetch_file(TRIP_UPDATES_URL, TRIP_UPDATES_FILE, max_age_seconds=10)

        # Pobieranie i rozpakowywanie danych statycznych GTFS
        fetch_file(GTFS_ZIP_URL, GTFS_ZIP_FILE, max_age_seconds=86400)  # Aktualizacja raz dziennie
        try:
            with zipfile.ZipFile(GTFS_ZIP_FILE, 'r') as zip_ref:
                zip_ref.extractall(GTFS_DIR)
            print(f"Pomyślnie rozpakowano {GTFS_ZIP_FILE} do {GTFS_DIR}.")
        except zipfile.BadZipFile:
            print(f"Błąd: Plik {GTFS_ZIP_FILE} jest uszkodzony lub nie jest plikiem ZIP.")
            # Spróbuj usunąć uszkodzony plik i pobrać ponownie przy następnym uruchomieniu
            os.remove(GTFS_ZIP_FILE)
            raise HTTPException(status_code=500, detail="Uszkodzony plik ZIP GTFS. Spróbuj ponownie.")
        except Exception as e:
            print(f"Błąd podczas rozpakowywania pliku ZIP GTFS: {e}")
            raise HTTPException(status_code=500, detail=f"Błąd podczas rozpakowywania pliku ZIP GTFS: {e}")

        # Wczytywanie danych statycznych
        shapes = load_csv_dict(os.path.join(GTFS_DIR, "shapes.txt"))
        trips = load_csv_dict(os.path.join(GTFS_DIR, "trips.txt"))

        # Tworzenie słownika shape_id -> lista punktów (posortowana po shape_pt_sequence)
        shapes_dict = {}
        for shape in shapes:
            shape_id = shape['shape_id']
            if shape_id not in shapes_dict:
                shapes_dict[shape_id] = []
            shapes_dict[shape_id].append({
                'lat': float(shape['shape_pt_lat']),
                'lon': float(shape['shape_pt_lon']),
                'seq': int(shape['shape_pt_sequence'])
            })

        # Sortowanie punktów w każdej trasie po shape_pt_sequence
        for shape_id in shapes_dict:
            shapes_dict[shape_id].sort(key=lambda x: x['seq'])
        data_store.shapes_dict = {
            s_id: [ShapePoint(**pt) for pt in s_pts]
            for s_id, s_pts in shapes_dict.items()
        }

        # Tworzenie słownika trip_id -> shape_id
        data_store.trips = trips
        data_store.trip_shape_map = {trip['trip_id']: trip['shape_id'] for trip in trips}

        # Wczytywanie danych GTFS-Realtime
        vehicle_feed = load_feed(VEHICLE_POSITIONS_FILE)
        trip_update_feed = load_feed(TRIP_UPDATES_FILE)

        # Tworzenie słownika opóźnień na podstawie trip_updates
        trip_delays = {}
        for entity in trip_update_feed.entity:
            if entity.HasField('trip_update'):
                trip_update = entity.trip_update
                trip_id = trip_update.trip.trip_id
                if trip_update.stop_time_update:
                    if trip_update.stop_time_update[0].HasField('arrival') and \
                       trip_update.stop_time_update[0].arrival.HasField('delay'):
                        delay = trip_update.stop_time_update[0].arrival.delay
                        trip_delays[trip_id] = delay
                    elif trip_update.stop_time_update[0].HasField('departure') and \
                         trip_update.stop_time_update[0].departure.HasField('delay'):
                        delay = trip_update.stop_time_update[0].departure.delay
                        trip_delays[trip_id] = delay
        data_store.trip_delays = trip_delays

        # Przetwarzanie danych o pojazdach
        current_vehicle_positions = []
        for entity in vehicle_feed.entity:
            if entity.HasField('vehicle'):
                vehicle = entity.vehicle
                trip_id = vehicle.trip.trip_id
                route_id = vehicle.trip.route_id
                vehicle_id = vehicle.vehicle.id
                position = vehicle.position

                delay = data_store.trip_delays.get(trip_id)
                shape_id = data_store.trip_shape_map.get(trip_id)
                shape_points_raw = data_store.shapes_dict.get(shape_id, [])
                shape_points_list = [{'lat': p.lat, 'lon': p.lon, 'seq': p.seq} for p in shape_points_raw]

                current_vehicle_positions.append(
                    VehicleData(
                        vehicle_id=vehicle_id,
                        route_id=route_id,
                        trip_id=trip_id,
                        position=Position(latitude=position.latitude, longitude=position.longitude),
                        delay_seconds=delay,
                        shape_id=shape_id,
                        shape_points=shape_points_list
                    )
                )
        data_store.vehicle_positions = current_vehicle_positions
        data_store.last_updated = time.time()
        print("Pomyślnie przetworzono wszystkie dane.")

    except HTTPException:
        # Przekazanie HTTPException dalej
        raise
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd podczas przetwarzania danych: {e}")
        raise HTTPException(status_code=500, detail=f"Wystąpił nieoczekiwany błąd serwera: {e}")


@app.on_event("startup")
async def startup_event():
    """
    Funkcja uruchamiana przy starcie aplikacji.
    Pobiera i przetwarza dane początkowe.
    """
    print("Aplikacja FastAPI uruchomiona. Rozpoczynam wstępne ładowanie danych...")
    await fetch_and_process_data()
    print("Wstępne ładowanie danych zakończone.")


@app.get("/", summary="Sprawdzenie statusu API", response_model=Dict[str, str])
async def read_root():
    """
    Endpoint do sprawdzenia, czy API działa.
    """
    return {"message": "GTFS Poznań Realtime API działa!", "status": "ok"}

@app.get("/vehicles", response_model=List[VehicleData], summary="Pobierz aktualne pozycje pojazdów")
async def get_vehicle_positions():
    """
    Zwraca aktualne pozycje pojazdów wraz z informacjami o trasach, opóźnieniach i punktach tras.
    """
    if not data_store.vehicle_positions:
        raise HTTPException(status_code=404, detail="Brak danych o pojazdach. Spróbuj zaktualizować dane.")
    return data_store.vehicle_positions

@app.post("/update_data", summary="Wymuś aktualizację danych GTFS", status_code=202)
async def update_data(background_tasks: BackgroundTasks):
    """
    Wymusza ponowne pobranie i przetworzenie wszystkich danych GTFS (statycznych i w czasie rzeczywistym)
    w tle. Odpowiedź jest zwracana natychmiast, a aktualizacja odbywa się asynchronicznie.
    """
    print("Żądanie aktualizacji danych odebrane. Uruchamiam zadanie w tle.")
    background_tasks.add_task(fetch_and_process_data)
    return {"message": "Aktualizacja danych GTFS została uruchomiona w tle. Dane będą dostępne wkrótce."}

@app.get("/data_status", summary="Sprawdź status ostatniej aktualizacji danych", response_model=Dict[str, Optional[str]])
async def get_data_status():
    """
    Zwraca czas ostatniej udanej aktualizacji danych.
    """
    if data_store.last_updated:
        return {"last_updated": time.ctime(data_store.last_updated)}
    return {"last_updated": None}

