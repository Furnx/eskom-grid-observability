#!/usr/bin/env python3
"""
lookup_area_ids.py
──────────────────
One-time helper to resolve a city/suburb name to its EskomSePush API area_id.
Run this manually whenever you want to add a new area to areas_config.yml.

Usage:
    python scripts/lookup_area_ids.py "Pretoria"
    python scripts/lookup_area_ids.py "Sandton"

Output:
    Prints the top matching area IDs and names so you can pick the right one
    and paste it into areas_config.yml.

API cost: 1 request per invocation (does not count against pipeline quota).
"""

import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("ESKOM_API_KEY")

if not API_KEY:
    print("ERROR: ESKOM_API_KEY not found in .env file.")
    sys.exit(1)

if len(sys.argv) < 2:
    print("Usage: python scripts/lookup_area_ids.py <search term>")
    sys.exit(1)

search_text = " ".join(sys.argv[1:])
url = f"https://developer.sepush.co.za/business/3.0/areas_search?text={search_text.replace(' ', '%20')}"

print(f"\nSearching for: '{search_text}'")
print(f"URL: {url}\n")

response = requests.get(url, headers={"token": API_KEY})

if response.status_code == 429:
    print("ERROR: API rate limit exceeded (HTTP 429). Try again tomorrow.")
    sys.exit(1)

if response.status_code != 200:
    print(f"ERROR: API returned HTTP {response.status_code}")
    print(response.text)
    sys.exit(1)

areas = response.json().get("areas", [])

if not areas:
    print("No areas found. Try a different search term.")
    sys.exit(0)

print(f"{'area_id':<55} {'name'}")
print("-" * 80)
for area in areas[:15]:
    print(f"{area['id']:<55} {area.get('name', 'N/A')}")

print(f"\nFound {len(areas)} result(s). Copy the desired area_id into areas_config.yml.")
