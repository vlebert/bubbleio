import requests
import logging


class Bubblepy:
    api_key = ""
    api_root = ""
    logger = logging.getLogger(__name__)

    def __init__(self, api_key, api_root):
        self.api_key = api_key
        self.api_root = api_root

    def headers(self):
        return {"Authorization": "Bearer " + self.api_key}

    def get_table(self, tablename):
        r = requests.get(self.api_root + "/" + tablename, headers=self.headers())
        remaining = r.json()["response"]["remaining"]
        records = r.json()["response"]["results"]

        cursor = 100

        while remaining > 0:
            params = {"cursor": cursor}
            self.logger.info(
                "Querying table %s,  : %s items remaining" % (tablename, remaining)
            )
            r = requests.get(
                self.api_root + "/etapeprojet", headers=self.headers(), params=params
            )
            records.append(r.json()["response"]["results"])
            cursor = cursor + 100
            remaining = r.json()["response"]["remaining"]

        return records
