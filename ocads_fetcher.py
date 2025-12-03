import requests
import json
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
if not os.path.exists('.env'):
    raise FileNotFoundError('.env file not found. Please create a .env file by copying .env.example and configuring it.')
load_dotenv()

BASE_URL = "https://www.ncei.noaa.gov/metadata/geoportal/opensearch"

OCADS_QUERY = (
    '("Carbon Dioxide Information Analysis Center" OR '
    '"Biological and Chemical Oceanography Data Management Office" OR '
    '"CLIVAR and Carbon Hydrographic Data Office" OR '
    '"Ocean Acidification Data Stewardship (OADS) Project" OR '
    '"Ocean Carbon and Acidification Data System (OCADS)" OR '
    '"Ocean Carbon Data System (OCADS) Project" OR '
    '"US DOC; NOAA; Office of Oceanic and Atmospheric Research; Ocean Acidification Program (OAP)")'
)

def fetch_ocads_results(bbox=None, extra_terms=None, keywords=None, time_range=None, filename="ocads_results.json"):
    results = []
    start = 1
    num = 25
    query_parts = [OCADS_QUERY]
    if extra_terms:
        extra_query = " AND ".join([f"\"{term}\"" for term in extra_terms])
        query_parts.append(extra_query)
    if keywords:
        keywords_query = f'keywords_s:"{keywords}"'
        query_parts.append(keywords_query)
    full_query = " AND ".join(query_parts)
    headers = {
        "User-Agent": "CIOOS MirrorBot",
        "Accept": "application/json"
    }
    while True:
        params = {"q": full_query, "start": start, "num": num, "f": "json"}
        if bbox:
            params["bbox"] = bbox
        if time_range:
            params["time"] = time_range
        print("Requesting URL:", requests.Request("GET", BASE_URL, params=params).prepare().url)
        delay = 1
        max_delay = 60
        while True:
            try:
                response = requests.get(BASE_URL, params=params, headers=headers)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as err:
                if response.status_code == 503:
                    print(f"503 error encountered. Waiting {delay} seconds before retrying.")
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
                    continue
                else:
                    raise err
        data = response.json()
        print("Response keys:", list(data.keys()))
        current_results = data.get("results", [])
        if not current_results:
            print("No more results found.")
            break
        results.extend(current_results)
        next_start = data.get("nextStart")
        if next_start is None or next_start < 0:
            break
        start = next_start
        time.sleep(1)
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False, indent=2)
    print(f"Results saved to {filename}")
    return filename

if __name__ == "__main__":
    # Load configuration from environment variables
    bbox = os.getenv("BBOX")
    extra_terms_str = os.getenv("EXTRA_TERMS", "")
    extra_terms = [term.strip() for term in extra_terms_str.split(",") if term.strip()]
    keywords = os.getenv("KEYWORDS")
    time_range = os.getenv("TIME_RANGE")
    filename = os.getenv("RESULTS_FILENAME", "ocads_results.json")
    
    fetch_ocads_results(
        bbox=bbox,
        extra_terms=extra_terms if extra_terms else None,
        keywords=keywords,
        time_range=time_range,
        filename=filename
    )