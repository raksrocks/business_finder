import requests
import sqlite3
import time

# --- Configuration ---
API_KEY = "your_key" 
DB_FILE = "places_data.db"

# --- Grid Generation Configuration --- 17.51537789180513, 78.30337750411357
CENTER_LAT = 17.5154
CENTER_LNG = 78.3034
GRID_COUNT = 5  # Creates a 5x5 grid (25 total points)
SPACING = 0.0009 # Approx 550 meters. Adjust for denser or wider searches.

def setup_database(conn):
    """Creates all necessary database tables."""
    cursor = conn.cursor()
    # Table for places data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS places (
            place_id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')
    # Table for place types
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS types (
            type_id INTEGER PRIMARY KEY AUTOINCREMENT,
            type_name TEXT NOT NULL UNIQUE
        )
    ''')
    # Junction table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS place_types (
            place_id TEXT,
            type_id INTEGER,
            PRIMARY KEY (place_id, type_id),
            FOREIGN KEY (place_id) REFERENCES places (place_id),
            FOREIGN KEY (type_id) REFERENCES types (type_id)
        )
    ''')
    # New table to store and track search grid locations
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_grid (
            location TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending' -- 'pending' or 'completed'
        )
    ''')
    conn.commit()
    print("Database tables are set up.")

def populate_search_grid(conn):
    """Generates and stores the grid of search locations."""
    cursor = conn.cursor()
    locations_to_insert = []
    # The grid is centered, so we start from a negative offset
    offset = (GRID_COUNT - 1) / 2
    
    for i in range(GRID_COUNT):
        for j in range(GRID_COUNT):
            lat = CENTER_LAT + (i - offset) * SPACING
            lng = CENTER_LNG + (j - offset) * SPACING
            location_str = f"{lat:.6f},{lng:.6f}"
            locations_to_insert.append((location_str,))
            
    # Use INSERT OR IGNORE to add only new locations
    cursor.executemany("INSERT OR IGNORE INTO search_grid (location) VALUES (?)", locations_to_insert)
    conn.commit()
    print(f"Grid populated. Total search points in DB: {cursor.lastrowid}")

def get_pending_locations(conn):
    """Fetches all locations from the grid that are marked as 'pending'."""
    cursor = conn.cursor()
    cursor.execute("SELECT location FROM search_grid WHERE status = 'pending'")
    # The result of fetchall is a list of tuples, e.g., [('lat,lng',), ...]. We extract the first element.
    return [row[0] for row in cursor.fetchall()]

def update_location_status(conn, location, status):
    """Updates the status of a location in the search_grid table."""
    cursor = conn.cursor()
    cursor.execute("UPDATE search_grid SET status = ? WHERE location = ?", (status, location))
    conn.commit()

# --- Functions to fetch and store place data (no changes needed) ---
def store_place_data(conn, places_data):
    """Stores place information in the normalized three-table structure."""
    cursor = conn.cursor()
    new_places_count = 0
    for place in places_data:
        place_id = place.get('place_id')
        name = place.get('name')
        types_list = place.get('types', [])
        if not place_id or not name: continue
        cursor.execute("INSERT OR IGNORE INTO places (place_id, name) VALUES (?, ?)", (place_id, name))
        if cursor.rowcount > 0: new_places_count += 1
        for type_name in types_list:
            cursor.execute("INSERT OR IGNORE INTO types (type_name) VALUES (?)", (type_name,))
            cursor.execute("SELECT type_id FROM types WHERE type_name = ?", (type_name,))
            type_id = cursor.fetchone()[0]
            cursor.execute("INSERT OR IGNORE INTO place_types (place_id, type_id) VALUES (?, ?)", (place_id, type_id))
    conn.commit()
    print(f"Processed {len(places_data)} places and added {new_places_count} new unique places to the database.")

def fetch_nearby_places(location, api_key):
    """Fetches data from the Google Maps API, handling pagination."""
    base_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {"location": location, "radius": 1500, "key": api_key}
    all_places = []
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        if data['status'] == 'OK':
            all_places.extend(data['results'])
            next_page_token = data.get('next_page_token')
            for _ in range(3):
                if not next_page_token: break
                time.sleep(2)
                pag_params = {'pagetoken': next_page_token, 'key': api_key}
                response = requests.get(base_url, params=pag_params)
                response.raise_for_status()
                data = response.json()
                if data['status'] == 'OK':
                    all_places.extend(data['results'])
                    next_page_token = data.get('next_page_token')
                else: break
            return all_places
        else:
            print(f"API Error: {data.get('status')}, {data.get('error_message', '')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Network Error: {e}")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    connection = sqlite3.connect(DB_FILE)
    setup_database(connection)
    
    # 1. Generate grid points and ensure they are in the database
    populate_search_grid(connection)
    
    # 2. Get the list of locations that still need to be searched
    locations_to_search = get_pending_locations(connection)
    
    if not locations_to_search:
        print("All search locations have already been completed.")
    else:
        print(f"Found {len(locations_to_search)} pending locations to search.")
    
    # 3. Loop through each pending location and process it
    for i, location in enumerate(locations_to_search):
        print(f"\n--- Processing location {i + 1}/{len(locations_to_search)}: {location} ---")
        
        places_results = fetch_nearby_places(location, API_KEY)
        
        if places_results:
            print(f"Total places fetched from API: {len(places_results)}")
            store_place_data(connection, places_results)
            # 4. Mark the location as 'completed' after successful processing
            update_location_status(connection, location, 'completed')
            print(f"Marked location {location} as completed.")
        else:
            print(f"Failed to fetch data for location {location}. It will be tried again next time.")

    connection.close()
    print("\n--- Script finished. Database connection closed. ---")
