import base64
import json
import gspread
import requests
import logging
import sys
from bubbleio import Bubbleio
from oauth2client.client import AccessTokenCredentials

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    print(event["data"])
    j = json.loads(base64.b64decode(event["data"]).decode("utf-8"))

    BUBBLE_API_KEY = j["bubble_api_key"]
    BUBBLE_API_ROOT = j["bubble_root"]
    BUBBLE_TABLE = j["bubble_table"]
    BUBBLE_LIST_COLUMNS = j["bubble_columns"]
    GDRIVE_TOKEN = j["gdrive_token"]
    GDRIVE_FOLDER = j["gdrive_folder"]
    SYNC_ID = j["task_id"]

    def progress_callback(progress):
        requests.post(
            "https://bubble-sync.bubbleapps.io/version-test/api/1.1/wf/updatetask",
            json={"taskId": SYNC_ID, "progress": progress},
        )

    bbio = Bubbleio(BUBBLE_API_KEY, BUBBLE_API_ROOT)
    df = bbio.get_all_results_as_df(BUBBLE_TABLE, progress_callback=progress_callback)

    columns = [col for col in BUBBLE_LIST_COLUMNS if col in df.columns.tolist()]
    df = df[["_id"] + columns]

    credentials = AccessTokenCredentials(GDRIVE_TOKEN, None)
    gc = gspread.authorize(credentials)

    sh = gc.create(BUBBLE_TABLE, folder_id=GDRIVE_FOLDER)
    ws = sh.get_worksheet(0)

    result = ws.update(
        [df.columns.values.tolist()] + df.fillna("").applymap(str).values.tolist()
    )
    requests.post(
        "https://bubble-sync.bubbleapps.io/version-test/api/1.1/wf/updatetask",
        json={
            "taskId": SYNC_ID,
            "rowCount": (result["updatedRows"] - 1),
            "status": ["✅ Finished"],
        },
    )

    print("done")
