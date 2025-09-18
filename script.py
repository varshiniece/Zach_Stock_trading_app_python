import requests
import csv
import os
import time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
LIMIT = 100
BASE_URL = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}'

tickers = []
url = BASE_URL
request_count = 1

while url:
    print(f'Requesting: {url}')
    response = requests.get(url)
    data = response.json()

    # Handle rate limit error
    if 'error' in data and 'exceeded' in data['error']:
        print(f"Rate limit hit: {data['error']}")
        print("Sleeping for 60 seconds before retrying...")
        time.sleep(60)
        continue  # Retry the same URL after waiting

    if 'results' in data:
        tickers.extend(data['results'])
    else:
        print(f"No results found or unexpected data: {data}")
        break

    url = data.get('next_url')
    if url:
        url += f"&apiKey={API_KEY}"  # Ensure API key is always present

    request_count += 1

    # Pause every 5 requests to stay safe
    if request_count % 5 == 0:
        print("Pausing for 10 seconds to avoid rate limit...")
        time.sleep(10)

# Example schema
example_ticker = {
    'ticker': 'ZWS',
    'name': 'Zurn Elkay Water Solutions Corporation',
    'market': 'stocks',
    'locale': 'us',
    'primary_exchange': 'XNYS',
    'type': 'CS',
    'active': True,
    'currency_name': 'usd',
    'cik': '0001439288',
    'composite_figi': 'BBG000H8R0N8',
    'share_class_figi': 'BBG001T36GB5',
    'last_updated_utc': '2025-09-11T06:11:10.586204443Z'
}

# Write to CSV
fieldnames = list(example_ticker.keys())
output_csv = 'tickers.csv'

with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for t in tickers:
        row = {key: t.get(key, '') for key in fieldnames}
        writer.writerow(row)

print(f'Wrote {len(tickers)} tickers to {output_csv}')
