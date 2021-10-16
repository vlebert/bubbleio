"""
This module contans Bubbleio class to handel Bubble.io API.

https://manual.bubble.io/core-resources/api/data-api
"""

import requests
import logging


class Bubbleio:
    # api_key = ""
    # api_root = ""
    logger = logging.getLogger(__name__)

    def __init__(self, api_key, api_root):
        """Instantiate a Bubbleio object

        Args:
            api_key (str): Your Bubble.io API Key to the specific app. Don't forget to
                           enable API in yout Bubble app in `Settings > API`. You can also get an
                           API token from there.
            api_root (str): The root URL of the API. Currently, the API root is generally
                            `https://appname.bubbleapps.io/api/1.1/obj`. It can also depends if you
                            have a custom domain name.
            Returns:
                Bubbleio: Instance of Bubbleio.
        """
        self.api_key = api_key
        self.api_root = api_root

    def headers(self):
        """Returns headers including authentication"""
        return {"Authorization": "Bearer " + self.api_key}

    def get(self, typename, limit=None, cursor=None):
        """Use this call to retrieve a list of things of a given type.
        By default, all GET requests return the first 100 items in the list, unless you
        a lesser `limit` argument.

        Args:
            typename (str): The type of "things" you are querying.
            limit (str): Use `limit` to specify the number of items you want in the response.
                         The default and maximum allowed is 100. Use cursor to specify where to start.
            cursor (str): This is the rank of the first item in the list.

        Returns:
            Dict: Returns a decoded JSON as documented in Bubble.io API documentation :

                    - 'cursor': This is the rank of the first item in the list,
                    - 'results': The list of the results,
                    - 'count': This is the number of items in the current response,
                    - 'remaining': his is the number of remaining items after the current response. Use this for the next call
        """
        params = {}
        if limit:
            params["limit"] = limit

        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            self.api_root + "/" + typename, headers=self.headers(), params=params
        )
        return r.json()["response"]

    def get_all(
        self,
        typename,
    ):
        """Get all intems of one "things" type. The function iterates with limit and cursor
        to get all items

        Args:
            typename (str): The type of "things" you are querying.

        Returns:
            List: The list of all items of the type.
        """
        response = self.get(typename)

        remaining = response["remaining"]
        records = response["results"]

        cursor = 100  # Because default limit value is 100

        while remaining > 0:
            self.logger.info(
                "Querying table %s,  : %s items remaining" % (typename, remaining)
            )
            response = self.get(typename, cursor=cursor)
            records.append(response["results"])
            cursor = cursor + 100
            remaining = response["remaining"]

        return records
