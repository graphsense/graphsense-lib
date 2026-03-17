# MultiInputChangeHeuristic


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**summary** | [**List[AddressOutput]**](AddressOutput.md) |  | 
**confidence** | **int** |  | [optional] [default to 50]

## Example

```python
from graphsense.models.multi_input_change_heuristic import MultiInputChangeHeuristic

# TODO update the JSON string below
json = "{}"
# create an instance of MultiInputChangeHeuristic from a JSON string
multi_input_change_heuristic_instance = MultiInputChangeHeuristic.from_json(json)
# print the JSON string representation of the object
print(MultiInputChangeHeuristic.to_json())

# convert the object into a dict
multi_input_change_heuristic_dict = multi_input_change_heuristic_instance.to_dict()
# create an instance of MultiInputChangeHeuristic from a dict
multi_input_change_heuristic_from_dict = MultiInputChangeHeuristic.from_dict(multi_input_change_heuristic_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


