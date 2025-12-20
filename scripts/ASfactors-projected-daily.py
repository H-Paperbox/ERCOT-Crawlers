import requests
from pathlib import Path

# =========================
# 已验证可用的接口
# =========================
LIST_URL = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS"
LIST_PARAMS = {
    "reportTypeId": "24886"
}

DOWNLOAD_URL = "https://www.ercot.com/misdownload/servlets/mirDownload"

# =========================
# 本地路径
# =========================
SAVE_DIR = Path("data/as_factors_projected")
SAVE_DIR.mkdir(exist_ok=True)

RECORD_FILE = Path("data/as_factors_projected/downloaded_docids.txt")
RECORD_FILE.touch(exist_ok=True)

downloaded = set(RECORD_FILE.read_text().splitlines())

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.ercot.com/",
}

# =========================
# 1. 拉文档列表
# =========================
resp = requests.get(
    LIST_URL,
    params=LIST_PARAMS,
    headers=HEADERS,
    timeout=30
)
resp.raise_for_status()

docs = resp.json()["ListDocsByRptTypeRes"]["DocumentList"]

# =========================
# 2. 只下载“新的 CSV”
# =========================
new_count = 0

for item in docs:
    doc = item["Document"]

    # 只要 CSV
    if not doc["FriendlyName"].endswith("_csv"):
        continue

    doc_id = doc["DocID"]

    # 已下载过 → 跳过
    if doc_id in downloaded:
        continue

    filename = doc["ConstructedName"]
    print(f"[NEW] Downloading {filename}")

    r = requests.get(
        DOWNLOAD_URL,
        params={"doclookupId": doc_id},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=60
    )
    r.raise_for_status()

    save_path = SAVE_DIR / filename
    with open(save_path, "wb") as f:
        f.write(r.content)

    # 记录下来，防止下次重复
    with open(RECORD_FILE, "a") as f:
        f.write(doc_id + "\n")

    downloaded.add(doc_id)
    new_count += 1

    print(f"[SAVED] {save_path}")

print(f"Done. New files downloaded: {new_count}")
