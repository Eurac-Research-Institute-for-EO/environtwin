import requests
import os
from requests.auth import HTTPBasicAuth

# ============ CONFIGURATION ============

USERNAME = "hdierkes"
PASSWORD = "!C0mP7t382232025"

# Dataset IDs
PRODUCTS = {
"FSC_20m": "EO:CRYO:DAT:HRSI:FSC",
"GFSC_60m": "EO:CRYO:DAT:HRSI:GFSC"
}


# Bounding box for South Tyrol (lon_min, lat_min, lon_max, lat_max)
BBOX = [10.3, 45.7, 12.5, 47.1]


# Date range
START_DATE = "2025-09-01"
END_DATE = "2025-09-05"


# Output directory
OUTPUT_DIR = "/mnt/CEPH_PROJECTS/Environtwin/test"


# CLMS API endpoint
API_URL = "https://clmsdataservices.eea.europa.eu/auxiliary/download"


session = requests.Session()
session.auth = HTTPBasicAuth(USERNAME, PASSWORD)


def get_download_links(product_id, bbox, start_date, end_date):
	params = {
		"datasetId": product_id,
		"bbox": ",".join(map(str, bbox)),
		"startDate": start_date,
		"endDate": end_date,
		"format": "GeoTIFF"
	}
	r = session.get(API_URL, params=params)
	r.raise_for_status()
	return r.json()


def download_file(url, outdir=OUTPUT_DIR):
	os.makedirs(outdir, exist_ok=True)
	filename = os.path.join(outdir, url.split("/")[-1])
	print(f"Downloading {filename} ...")
	r = requests.get(url, stream=True)
	r.raise_for_status()
	with open(filename, "wb") as f:
		for chunk in r.iter_content(1024*1024):
			f.write(chunk)
	print("✅ Done:", filename)


if __name__ == "__main__":
	for name, pid in PRODUCTS.items():
		print(f"\nQuerying {name} ({pid})...")
		links = get_download_links(pid, BBOX, START_DATE, END_DATE)
		for entry in links.get("products", []):
			url = entry.get("url")
			if url:
				download_file(url)
