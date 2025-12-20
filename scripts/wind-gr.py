import requests
from pathlib import Path
from datetime import datetime
import pytz
import zipfile
import pandas as pd

# =========================
# ERCOT 接口（已验证）
# =========================
LIST_URL = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS"
LIST_PARAMS = {"reportTypeId": "14787"}  # Wind Power Production

DOWNLOAD_URL = "https://www.ercot.com/misdownload/servlets/mirDownload"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.ercot.com/",
}

# =========================
# 本地存储
# =========================
BASE_DIR = Path("data/wind_hourly_raw")
BASE_DIR.mkdir(parents=True, exist_ok=True)

RECORD_FILE = BASE_DIR / "downloaded_dates.txt"
RECORD_FILE.touch(exist_ok=True)

downloaded_dates = set(RECORD_FILE.read_text().splitlines())

# ERCOT 时间
ERCOT_TZ = pytz.timezone("US/Central")

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
# 2. 只选 “00:00 对应文件”
# =========================
new_count = 0

for item in docs:
    doc = item["Document"]

    # 只要 CSV
    if not doc["FriendlyName"].endswith("_csv"):
        continue

    # 解析发布时间
    publish_dt = datetime.fromisoformat(
        doc["PublishDate"].replace("Z", "")
    )
    publish_dt = publish_dt.astimezone(ERCOT_TZ)

    # 只取 00:xx 发布的
    if publish_dt.hour != 0:
        continue

    delivery_date = publish_dt.date().isoformat()

    # 已经下载过这一天 → 跳过
    if delivery_date in downloaded_dates:
        continue

    # =====================
    # 下载
    # =====================
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

    # 记录 date（不是 DocID）
    with open(RECORD_FILE, "a") as f:
        f.write(delivery_date + "\n")

    downloaded_dates.add(delivery_date)
    new_count += 1

    print(f"[SAVED] {save_path}")

    # ===== 解压 zip（zip 里只有一个 csv）=====
    with zipfile.ZipFile(save_path, "r") as z:
        csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
        z.extract(csv_name, save_path.parent)

    csv_path = save_path.parent / csv_name

    # ===== 读 csv =====
    df = pd.read_csv(csv_path)

    # ===== 只保留第 24–72 小时 =====
    df_keep = df.iloc[24:72].copy()

    # ===== 覆盖保存（或另存）=====
    df_keep.to_csv(csv_path, index=False)

    # （可选）删掉 zip
    save_path.unlink()


print(f"Done. New days downloaded: {new_count}")
