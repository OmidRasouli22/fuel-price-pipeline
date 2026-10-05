from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
# access the secret from the Databricks secret scope
api_key = w.dbutils.secrets.get(scope="fuel", key="TANKERKOENIG_API_KEY")

import requests

import requests

response = requests.get(
    "https://creativecommons.tankerkoenig.de/json/list.php",
    params={
        "lat": 47.999,
        "lng": 7.842,
        "rad": 10,
        "sort": "dist",
        "type": "all",
        "apikey": api_key,
    },
    timeout=30,
)
response.raise_for_status()
payload = response.json()
if not payload.get("ok"):
    raise RuntimeError(payload.get("message"))

# write the raw response to the landing volume
import io
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
path = f"/Volumes/fuel/raw/landing/prices/{now:%Y-%m-%d}/{now:%H%M%S}.json"
w.files.upload(path, io.BytesIO(response.content), overwrite=True)
print(f"Wrote {len(payload['stations'])} stations to {path}")
