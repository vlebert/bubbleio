"""
This module contans Bubbleio class to handle Bubble.io API.

https://manual.bubble.io/core-resources/api/data-api
"""

import requests
import logging
import pandas as pd


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

    def get(self, typename, limit=None, cursor=None):
        """Python implementation of Bubble.io GET API call.

        Use this call to retrieve a list of things of a given type.
        By default, all GET requests return the first 100 items in the list, unless you
        a lesser `limit` argument.

        Args:
            typename (str): The type of "things" you are querying.
            limit (str): Use `limit` to specify the number of items you want in the response.
                         The default and maximum allowed is 100. Use cursor to specify where to start.
            cursor (str): This is the rank of the first item in the list.

        Returns:
            Dict: Returns a decoded JSON (dict) as documented in Bubble.io API documentation. Items of the dict:

                    - cursor: This is the rank of the first item in the list,
                    - results: The list of the results,
                    - count: This is the number of items in the current response,
                    - remaining: his is the number of remaining items after the current response. Use this for the next call

        Examples:

                >>> from bubbleio import Bubbleio
                >>> API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                >>> API_ROOT = "https://appname.bubbleapps.io/api/1.1/obj"
                >>> bbio = Bubbleio(API_KEY, API_ROOT)
                >>> bbio.get("foo_type")
                {
                    "cursor": 0,
                    "results": [
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
                    ],
                    "remaining": 0,
                    "count": 31
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

        r = requests.get(
            self.api_root + "/" + typename, headers=self.headers(), params=params
        )
        return r.json()["response"]

    def get_results(self, typename, limit=None, cursor=None):
        """Same as get() method, but returns only the results.

        Args:
            typename (str): The type of "things" you are querying.
            limit (str): Use `limit` to specify the number of items you want in the response.
                         The default and maximum allowed is 100. Use cursor to specify where to start.
            cursor (str): This is the rank of the first item in the list.

        Returns:
            List: The list of all items of the type.

        Examples:

                >>> from bubbleio import Bubbleio
                >>> API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                >>> API_ROOT = "https://appname.bubbleapps.io/api/1.1/obj"
                >>> bbio = Bubbleio(API_KEY, API_ROOT)
                >>> bbio.get_results("foo_type")
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
        return self.get(typename, limit=limit, cursor=cursor)["results"]

    def get_all_results(
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

    def get_results_as_df(
        self, typename, limit=None, cursor=None, list_fields=None, mask_fields=None
    ):
        """Returns results as a Pandas.DataFrame

        Args:
            typename (str): The type of "things" you are querying.
            limit (str): Use `limit` to specify the number of items you want in the response.
                         The default and maximum allowed is 100. Use cursor to specify where to start.
            cursor (str): This is the rank of the first item in the list.
            list_fields(list): The list of fields to returns in the DataFrame. All other fields will
                               be removed from the DataFrame. If None, all fields are returned.
                               Cant't be use in combination with mask_fields.
            mask_fields(list): The list of fields to remove from the DataFrame.
                               Cant't be use in combination with mask_fields.

        Returns:
            Pandas.DataFrame: The list of all items of the type.
        """
        if list_fields and mask_fields:
            raise ValueError(
                "get_results_as_df(): list_fields and mask_fields can't be used at the same time"
            )
        else:
            df = pd.DataFrame(self.get_retults(typename, limit=limit, cursor=cursor))
            if list_fields:
                df = df.loc[:, df.columns.isin(list_fields)]
            elif mask_fields:
                df = df.loc[:, ~df.columns.isin(mask_fields)]
        return df

    def get_all_results_as_df(
        self, typename, list_fields=None, mask_fields=None, joins=None
    ):
        """Returns all results as a Pandas.DataFrame

        Args:
            typename (str): The type of "things" you are querying..
            list_fields(list): The list of fields to returns in the DataFrame. All other fields will
                               be removed from the DataFrame. If None, all fields are returned. This
                               option also allow to order the columns.
                               Cant't be use in combination with mask_fields.
            mask_fields(list): The list of fields to remove from the DataFrame.
                               Cant't be use in combination with mask_fields.
            joins(list): List of dicts giving the parameters of the joins to be made. Dict must have
                         these Items :

                         - field: Name of the field referencing the foreign table (foreign key)
                         - typename: Name of the foreign table to be joined
                         - list_fields: same logic as above
                         - mask_fields: same logic as above

        Returns:
            Pandas.DataFrame: The list of all items of the type.
        """
        if list_fields and mask_fields:
            raise ValueError(
                "get_all_results_as_df(): list_fields and mask_fields can't be used at the same time"
            )
        else:
            df = pd.DataFrame(self.get_all_results(typename))
            if list_fields:
                df = df[list_fields]
            elif mask_fields:
                df = df.loc[:, ~df.columns.isin(mask_fields)]

            if joins:
                for j_param in joins:
                    foreign_table = self.get_all_results_as_df(
                        j_param["typename"],
                        list_fields=j_param.get("list_fields"),
                        mask_fields=j_param.get("mask_fields"),
                    )
                    df = df.merge(
                        foreign_table,
                        how="left",
                        left_on=j_param["field"],
                        right_on="_id",
                    )
                    df = df.drop(columns=[j_param["field"]])
        return df
