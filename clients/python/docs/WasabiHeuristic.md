# WasabiHeuristic


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**detected** | **bool** |  | 
**confidence** | **int** |  | 
**version** | **str** |  | 
**n_participants** | **int** |  | 
**denominations** | **List[int]** |  | 

## Example

```python
from graphsense.models.wasabi_heuristic import WasabiHeuristic

# TODO update the JSON string below
json = "{}"
# create an instance of WasabiHeuristic from a JSON string
wasabi_heuristic_instance = WasabiHeuristic.from_json(json)
# print the JSON string representation of the object
print(WasabiHeuristic.to_json())

# convert the object into a dict
wasabi_heuristic_dict = wasabi_heuristic_instance.to_dict()
# create an instance of WasabiHeuristic from a dict
wasabi_heuristic_from_dict = WasabiHeuristic.from_dict(wasabi_heuristic_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


