import requests
import sqlite3
import os
from dotenv import load_dotenv

# Fetch Data from EIA API
load_dotenv()
api_key = os.getenv("API_KEY")
url = f"https://api.eia.gov/v2/electricity/electric-power-operational-data/data/?api_key={api_key}&frequency=monthly&data[0]=cost&start=2024-01&end=2024-07&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

response = requests.get(url)
data = response.json()

# Connect to SQLite Database
conn = sqlite3.connect("electric_power_data.db")
cursor = conn.cursor()

# Create Table in SQLite
cursor.execute('''
    CREATE TABLE IF NOT EXISTS electric_power_data (
        period TEXT,
        location TEXT,
        state_description TEXT,
        sector_id INTEGER,
        sector_description TEXT,
        fuel_type_id TEXT,
        fuel_type_description TEXT,
        cost REAL
    )
''')

# Insert Data with Null Handling
if "response" in data and "data" in data["response"]:
    for item in data["response"]["data"]:
        period = item.get('period')
        location = item.get('location')
        state_description = item.get('stateDescription')
        sector_id = item.get('sectorid')
        sector_description = item.get('sectorDescription')
        fuel_type_id = item.get('fueltypeid')
        fuel_type_description = item.get('fuelTypeDescription')
        cost = item.get('cost', 0.0)  # Replace null cost with 0.0

        cursor.execute('''
            INSERT INTO electric_power_data (
                period, location, state_description, sector_id, sector_description, fuel_type_id, fuel_type_description, cost
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (period, location, state_description, sector_id, sector_description, fuel_type_id, fuel_type_description, cost))
else:
    print("Error: 'response' or 'data' key not found in the API response.")

# Commit the data insertion
conn.commit()

# Query Data Examples

# Query 1: Single average cost per fuel type (for example, 'bituminous coal')
cursor.execute("SELECT fuel_type_description, AVG(cost) FROM electric_power_data WHERE fuel_type_description = 'bituminous coal'")
print("Average cost for bituminous coal:", cursor.fetchone())

# Query 2: Single total cost for a specific sector (for example, 'Electric Utility')
cursor.execute("SELECT sector_description, SUM(cost) FROM electric_power_data WHERE sector_description = 'Electric Utility'")
print("Total cost for Electric Utility sector:", cursor.fetchone())

# Query 3: Count records per fuel type for July 2024
cursor.execute("SELECT fuel_type_description, COUNT(*) FROM electric_power_data WHERE period = '2024-07' GROUP BY fuel_type_description LIMIT 3")
print("Record count per fuel type in July 2024 (limited):", cursor.fetchall())

conn.close()
