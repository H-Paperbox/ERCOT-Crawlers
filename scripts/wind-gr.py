import requests
from pathlib import Path
from datetime import datetime
import pytz
import zipfile
import pandas as pd

# ======================== ERCOT Setup =========================
LIST_URL = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS"
LIST_PARAMS = {"reportTypeId": "14787"}  # Wind Power Production

DOWNLOAD_URL = "https://www.ercot.com/misdownload/servlets/mirDownload"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.ercot.com/",
}

# ========================= Local Setup =========================
BASE_DIR = Path("data/wind_hourly_raw")
BASE_DIR.mkdir(parents=True, exist_ok=True)

RECORD_FILE = BASE_DIR / "downloaded_dates.txt"
RECORD_FILE.touch(exist_ok=True)

downloaded_dates = set(RECORD_FILE.read_text().splitlines())

# ERCOT timezone
ERCOT_TZ = pytz.timezone("US/Central")

# ========================= Find all available documents =========================
resp = requests.get(
    LIST_URL,
    params=LIST_PARAMS,
    headers=HEADERS,
    timeout=30
)
resp.raise_for_status()

docs = resp.json()["ListDocsByRptTypeRes"]["DocumentList"]

# ========================= Download new documents only at 00:00 =========================
new_count = 0

for item in docs:
    doc = item["Document"]

    if not doc["FriendlyName"].endswith("_csv"):
        continue

    publish_dt = datetime.fromisoformat(
        doc["PublishDate"].replace("Z", "")
    )
    publish_dt = publish_dt.astimezone(ERCOT_TZ)

    if publish_dt.hour != 0:
        continue

    delivery_date = publish_dt.date().isoformat()

    if delivery_date in downloaded_dates:
        continue

    print(f"[NEW DAY] {delivery_date} -> {doc['ConstructedName']}")

    r = requests.get(
        DOWNLOAD_URL,
        params={"doclookupId": doc["DocID"]},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=60
    )
    r.raise_for_status()

    save_path = BASE_DIR / f"{delivery_date}.zip"
    with open(save_path, "wb") as f:
        f.write(r.content)

    # record the downloaded date
    with open(RECORD_FILE, "a") as f:
        f.write(delivery_date + "\n")

    downloaded_dates.add(delivery_date)
    new_count += 1

    print(f"[SAVED] {save_path}")

    # unzip
    with zipfile.ZipFile(save_path, "r") as z:
        csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
        z.extract(csv_name, save_path.parent)

    csv_path = save_path.parent / csv_name
 
    # leave only hours 24–72, 24 hours history and 24 hours forecast
    df = pd.read_csv(csv_path)
    df_keep = df.iloc[24:72].copy()
    df_keep.to_csv(csv_path, index=False)
    save_path.unlink()

print(f"Done. New days downloaded: {new_count}")
