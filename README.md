# Bubbleio Python API

## Project overview

This python package is a toolset to query Bubble.io API. This package uses [Pandas](https://pandas.pydata.org/)
DataFrame to returns results.

Pandas.DataFrame lets you query the data, process it, save it (ant later on uptate changes directly to Bubble app with
this package).

**⚠ Warning**: This project is quite new (october 2021), and breaking changes may appears until version 1.0

## Installation

```
python -m pip install -U pip
python -m pip install bubbleio
```
## Package documentation

 https://vlebert.github.io/bubbleio/

## Bubble.io API documentation

https://manual.bubble.io/core-resources/api/data-api

## Roadmap

* [GET](https://manual.bubble.io/core-resources/api/data-api#getting-a-list-of-things-and-search) function : **80%**
    * TODO: Sorting options
* TODO: [PATCH](https://manual.bubble.io/core-resources/api/data-api#modify-a-thing-by-id) function : **0%**
* TODO: Bulk [PATCH](https://manual.bubble.io/core-resources/api/data-api#modify-a-thing-by-id)
* ...

## Example of usage :

```python
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
```

```python
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
```
