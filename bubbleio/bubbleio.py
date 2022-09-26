"""
This module contans Bubbleio class to handle Bubble.io API.

https://manual.bubble.io/core-resources/api/data-api
"""

import requests
import logging
import pandas as pd
import json


class Bubbleio:
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

            Examples:

                >>> from bubbleio import Bubbleio
                >>> API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                >>> API_ROOT = "https://appname.bubbleapps.io/api/1.1/obj"
                >>> bbio = Bubbleio(API_KEY, API_ROOT)
        """
        self.api_key = api_key
        self.api_root = api_root
        self.logger = logging.getLogger(__name__)

    def headers(self):
        """Returns headers including authentication

        Note : considering passing as private method.

        Examples:

            >>> from bubbleio import Bubbleio
            >>> API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            >>> API_ROOT = "https://appname.bubbleapps.io/api/1.1/obj"
            >>> bbio = Bubbleio(API_KEY, API_ROOT)
            >>> bbio.header()
            {'Authorization': 'Bearer xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'}
        """
        return {"Authorization": "Bearer " + self.api_key}

    def get(self, typename, limit=None, cursor=None, constraints=None):
        """Python implementation of Bubble.io GET API call.

        Use this call to retrieve a list of things of a given type.
        By default, all GET requests return the first 100 items in the list, unless you
        a lesser `limit` argument.

        Args:
            typename (str): The type of "things" you are querying.
            limit (str): Use `limit` to specify the number of items you want in the response.
                         The default and maximum allowed is 100. Use cursor to specify where to start.
            cursor (str): This is the rank of the first item in the list.
            constraints (list): See https://manual.bubble.io/core-resources/api/data-api#search-constraints.
                                This parameter should be an list of constraints, e.g., objects with a ``key``,
                                ``constraint_type``, and most of the time a ``value``.


        Returns:
            Dict: Returns a decoded JSON (dict) as documented in Bubble.io API documentation. Items of the dict:

                    - ``cursor``: This is the rank of the first item in the list,
                    - ``results``: The list of the results,
                    - ``count``: This is the number of items in the current response,
                    - ``remaining``: his is the number of remaining items after the current response. Use this for the next call

        Examples:

            >>> from bubbleio import Bubbleio
            >>> API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            >>> API_ROOT = "https://appname.bubbleapps.io/api/1.1/obj"
            >>> bbio = Bubbleio(API_KEY, API_ROOT)
            >>> bbio.get("fooType")
            {
                "cursor": 0,
                "results": [
                    {
                        "foo_field_1": "value1",
                        "foo_field_2": "value2",
                        "_id": "item1_bubble_id"
                    },
                    {
                        "foo_field_1": "value3",
                        "foo_field_2": "value4",
                        "_id": "item2_bubble_id"
                    },
                    ...
                ],
                "remaining": 0,
                "count": 31
            }

            >>> bbio.get("fooType", constraints=[
            ...     {"key": "foo_field_1", "value": "value1", "constraint_type":"equals"}
            ... ])
            {
                "cursor": 0,
                "results": [
                    {
                        "foo_field_1": "value1",
                        "foo_field_2": "value2",
                        "_id": "item1_bubble_id"
                    }
                ],
                "remaining": 0,
                "count": 1
            }
        """
        self.logger.debug(
            "GET call on type %s with limit %s and cursor %s"
            % (typename, limit, cursor)
        )
        params = {}
        if limit:
            params["limit"] = limit

        if cursor:
            params["cursor"] = cursor

        if constraints:
            params["constraints"] = json.dumps(constraints)

        r = requests.get(
            self.api_root + "/" + typename, headers=self.headers(), params=params
        )
        r.raise_for_status()
        return r.json()["response"]

    def get_results(self, typename, limit=None, cursor=None, constraints=None):
        """Same as get() method, but returns only the results.

        Args:
            typename (str): The type of "things" you are querying.
            limit (str): Use `limit` to specify the number of items you want in the response.
                         The default and maximum allowed is 100. Use cursor to specify where to start.
            cursor (str): This is the rank of the first item in the list.
            constraints (list): See https://manual.bubble.io/core-resources/api/data-api#search-constraints.
                                This parameter should be an list of constraints, e.g., objects with a ``key``,
                                ``constraint_type``, and most of the time a ``value``.
                                See :meth:`~bubbleio.bubbleio.Bubbleio.get` example.

        Returns:
            List: The list of all items of the type.

        Examples:

            >>> from bubbleio import Bubbleio
            >>> API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            >>> API_ROOT = "https://appname.bubbleapps.io/api/1.1/obj"
            >>> bbio = Bubbleio(API_KEY, API_ROOT)
            >>> bbio.get_results("fooType")
            [
                {
                    "foo_field_1": "value",
                    "foo_field_2": "value",
                    "_id": "item1_bubble_id"
                },
                {
                    "foo_field_1": "value",
                    "foo_field_2": "value",
                    "_id": "item2_bubble_id"
                },
                ...
            ]
        """
        return self.get(typename, limit=limit, cursor=cursor, constraints=constraints)[
            "results"
        ]

    def get_all_results(
        self,
        typename,
        constraints=None,
        progress_callback=None,
        progress_callback_resolution=0.1,
    ):
        """Get all intems of one "things" type. The function iterates with limit and cursor
        to get all items

        Args:
            typename (str): The type of "things" you are querying.
            constraints (list): See https://manual.bubble.io/core-resources/api/data-api#search-constraints.
                                This parameter should be an list of constraints, e.g., objects with a ``key``,
                                ``constraint_type``, and most of the time a ``value``.
                                See :meth:`~bubbleio.bubbleio.Bubbleio.get` example.
            progress_callback: Function to be called to communicate progress
            progress_callback_resolution: Parameter user to tune the frequency of callback_progress function.

        Returns:
            List: The list of all items of the type.
        """
        response = self.get(typename, constraints=constraints)
        cursor = 100  # Because default limit value is 100

        remaining = response["remaining"]
        records = response["results"]

        item_count = len(response["results"]) + response["remaining"]
        item_processed = len(response["results"])
        progress = item_processed / item_count
        progress_callback(progress)
        progress_buffer = progress  # to handle resolution

        self.logger.info("%s items in total. Processing..." % (item_count))

        while remaining > 0:
            response = self.get(typename, cursor=cursor, constraints=constraints)
            records.extend(response["results"])
            item_processed = cursor + len(response["results"])
            progress = item_processed / item_count

            if (
                (progress - progress_buffer) > progress_callback_resolution
            ) or progress == 1:
                progress_callback(progress)
                self.logger.info(
                    "Progress %s / %s. Callback sent" % (item_processed, item_count)
                )
                progress_buffer = progress

            cursor = cursor + 100
            remaining = response["remaining"]
            self.logger.debug(
                "Querying table %s  : %s items remaining" % (typename, remaining)
            )

        return records

    def get_results_as_df(self, typename, limit=None, cursor=None, constraints=None):
        """Returns results as a Pandas.DataFrame

        Args:
            typename (str): The type of "things" you are querying.
            limit (str): Use `limit` to specify the number of items you want in the response.
                         The default and maximum allowed is 100. Use cursor to specify where to start.
            cursor (str): This is the rank of the first item in the list.
            constraints (list): See https://manual.bubble.io/core-resources/api/data-api#search-constraints.
                                This parameter should be an list of constraints, e.g., objects with a ``key``,
                                ``constraint_type``, and most of the time a ``value``.
                                See :meth:`~bubbleio.bubbleio.Bubbleio.get` example.
            progress_callback: Function to be called to communicate progress
            progress_callback_resolution: Parameter user to tune the frequency of callback_progress function.

        Returns:
            Pandas.DataFrame: The list of all items of the type.

        Examples:

            >>> from bubbleio import Bubbleio
            >>> bbio = Bubbleio(API_KEY, API_ROOT)
            >>> bbio.get_results("fooType")
            _id     fooField1   fooField2   fooBar
            idFoo1  value       value       idBar1
            idFoo2  value       value       idBar2
        """
        df = pd.DataFrame(
            self.get_results(
                typename, limit=limit, cursor=cursor, constraints=constraints
            )
        )
        return df

    def get_all_results_as_df(
        self,
        typename,
        joins=None,
        constraints=None,
        progress_callback=None,
        progress_callback_resolution=0.1,
    ):
        """Returns all results as a Pandas.DataFrame

        Args:
            typename (str): The type of "things" you are querying..
            joins(list): List of dicts giving the parameters of the joins to be made. Dict must have
                         these Items :

                         - field: Name of the field referencing the foreign table (foreign key)
                         - typename: Name of the foreign table to be joined
            constraints (list): See https://manual.bubble.io/core-resources/api/data-api#search-constraints.
                                This parameter should be an list of constraints, e.g., objects with a ``key``,
                                ``constraint_type``, and most of the time a ``value``.
                                See :meth:`~bubbleio.bubbleio.Bubbleio.get` example.

        Returns:
            Pandas.DataFrame: The list of all items of the type.

        Examples:

            >>> from bubbleio import Bubbleio
            >>> bbio = Bubbleio(API_KEY, API_ROOT)
            >>> joins_ = [
            ...     {
            ...         "field": "fooBar",
            ...         "typename": "barType",
            ...     }
            ... ]
            >>> bbio.get_results("fooType")
            _id     fooField1   fooField2   fooBar
            idFoo1  value       value       idBar1
            idFoo2  value       value       idBar2
            >>> bbio.get_results("barType")
            _id     barField1   barField2
            idBar1  value       value
            idBar2  value       value
            >>> bbio.get_all_results_as_df("fooType", joins=joins_)
            _id     fooField1   fooField2   fooBar  fooBar__id      fooBar_barField1   fooBar_barField2
            idFoo1  value       value       idBar1  idBar1          value              value
            idFoo2  value       value       idBar2  idBar2          value              value
        """
        df = pd.DataFrame(
            self.get_all_results(
                typename,
                constraints=constraints,
                progress_callback=progress_callback,
                progress_callback_resolution=progress_callback_resolution,
            )
        )
        if joins:
            for j_param in joins:
                foreign_table = self.get_all_results_as_df(j_param["typename"])
                # Add prefix to avoid confusion
                prefix = j_param["field"] + "_"
                foreign_table = foreign_table.add_prefix(prefix)
                try:
                    df = df.merge(
                        foreign_table,
                        how="left",
                        left_on=j_param["field"],
                        right_on=prefix + "_id",
                    )
                except KeyError as e:
                    self.logger.warning("Join impossible (KeyError): %s" % (e))
        return df
