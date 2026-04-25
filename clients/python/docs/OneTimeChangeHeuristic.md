# OneTimeChangeHeuristic


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**summary** | [**List[AddressOutput]**](AddressOutput.md) |  | 
**confidence** | **int** |  | [optional] [default to 50]

## Example

```python
from graphsense.models.one_time_change_heuristic import OneTimeChangeHeuristic

# TODO update the JSON string below
json = "{}"
# create an instance of OneTimeChangeHeuristic from a JSON string
one_time_change_heuristic_instance = OneTimeChangeHeuristic.from_json(json)
# print the JSON string representation of the object
print(OneTimeChangeHeuristic.to_json())

# convert the object into a dict
one_time_change_heuristic_dict = one_time_change_heuristic_instance.to_dict()
# create an instance of OneTimeChangeHeuristic from a dict
one_time_change_heuristic_from_dict = OneTimeChangeHeuristic.from_dict(one_time_change_heuristic_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


