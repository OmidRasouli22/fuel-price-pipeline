import io
import sys
import time
from datetime import date, timedelta

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

BASE_URL = "https://data.tankerkoenig.de/tankerkoenig-organization/tankerkoenig-data/raw/branch/master"
LANDING = "/Volumes/fuel/raw/landing/history"

w = WorkspaceClient()
auth = (
    w.dbutils.secrets.get("fuel", "tankerkoenig_user"),
    w.dbutils.secrets.get("fuel", "tankerkoenig_api_key"),
)

if len(sys.argv) > 2:
    start = date.fromisoformat(sys.argv[1])
    end = date.fromisoformat(sys.argv[2])
else:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=6)

day = start
while day <= end:
    for kind in ("prices", "stations"):
        name = f"{day}-{kind}.csv"
        source = f"{BASE_URL}/{kind}/{day:%Y}/{day:%m}/{name}"
        target = f"{LANDING}/{kind}/{day:%Y}/{day:%m}/{name}"

        try:
            w.files.get_metadata(target)
            print(f"skip  {name}")
            continue
        except NotFound:
            pass

        response = requests.get(source, auth=auth, timeout=300)
        response.raise_for_status()
        w.files.upload(target, io.BytesIO(response.content), overwrite=True)
        print(f"saved {name} ({len(response.content) / 1e6:.1f} MB)")
        time.sleep(1)

    day += timedelta(days=1)
