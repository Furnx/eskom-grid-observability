import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("ESKOM_API_KEY")

if not API_KEY or API_KEY == "your_actual_api_key_here":
    print("Error: Please put your real API key in the .env file!")
    exit()

headers = {"token": API_KEY}

# Search for Tshwane to get its specific Area ID
search_text = "Tshwane"
search_url = f"https://developer.sepush.co.za/business/3.0/areas_search?text={search_text}"

print(f"Searching for {search_text} Area ID using v3.0 API...")
search_response = requests.get(search_url, headers=headers)

# NEW DEBUGGING SECTION
print(f"Status Code: {search_response.status_code}")
# print("Raw API Response Text:")
# print(repr(search_response.text))
# -----------------------------

try:
    search_data = search_response.json()
except requests.exceptions.JSONDecodeError:
    print("\nSTOPPING: The API returned something that isn't JSON. Look at the raw response text above to see what it is.")
    exit()

if "error" in search_data:
    print(f"\nAPI Error: {search_data['error']}")
    exit()

# Grab the first matching area's ID
try:
    area_id = search_data['areas'][0]['id']
    area_name = search_data['areas'][0]['name']
    print(f"\nFound Area: {area_name} (ID: {area_id})")
except (KeyError, IndexError):
    print("\nSTOPPING: The structure was off. Look at the raw response above to see why!")
    print(f"Parsed JSON data: {json.dumps(search_data, indent=2)}")
    exit()

schedule_url = f"https://developer.sepush.co.za/business/3.0/area?id={area_id}"
print(f"Fetching schedule for {area_name}...")

schedule_response = requests.get(schedule_url, headers=headers)
schedule_data = schedule_response.json()

if "error" in schedule_data:
    print(f"\nAPI Error on Schedule fetch: {schedule_data['error']}")
    exit()

# Save the raw JSON payload to Local Data Lake
os.makedirs("data", exist_ok=True)
file_path = "data/raw_tshwane_schedule.json"

with open(file_path, "w") as f:
    json.dump(schedule_data, f, indent=4)

print(f"Success! Raw schedule saved to {file_path}")