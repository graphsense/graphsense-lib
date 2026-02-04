# Stats

API statistics model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**currencies** | [**List[CurrencyStats]**](CurrencyStats.md) |  |
**version** | **str** |  | [optional]
**request_timestamp** | **str** |  | [optional]

## Example

```python
from graphsense.models.stats import Stats

# TODO update the JSON string below
json = "{}"
# create an instance of Stats from a JSON string
stats_instance = Stats.from_json(json)
# print the JSON string representation of the object
print(Stats.to_json())

# convert the object into a dict
stats_dict = stats_instance.to_dict()
# create an instance of Stats from a dict
stats_from_dict = Stats.from_dict(stats_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
