# DirectChangeHeuristic


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**summary** | [**List[AddressOutput]**](AddressOutput.md) |  | 
**confidence** | **int** |  | [optional] [default to 100]

## Example

```python
from graphsense.models.direct_change_heuristic import DirectChangeHeuristic

# TODO update the JSON string below
json = "{}"
# create an instance of DirectChangeHeuristic from a JSON string
direct_change_heuristic_instance = DirectChangeHeuristic.from_json(json)
# print the JSON string representation of the object
print(DirectChangeHeuristic.to_json())

# convert the object into a dict
direct_change_heuristic_dict = direct_change_heuristic_instance.to_dict()
# create an instance of DirectChangeHeuristic from a dict
direct_change_heuristic_from_dict = DirectChangeHeuristic.from_dict(direct_change_heuristic_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


