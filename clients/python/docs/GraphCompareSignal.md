# GraphCompareSignal

One row of the pairwise comparison table; values stringified per tx.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**kind** | **str** |  | 
**per_tx** | **List[Optional[str]]** |  | 
**verdict** | **str** |  | 
**weight** | **int** |  | [optional] [default to 0]

## Example

```python
from graphsense.models.graph_compare_signal import GraphCompareSignal

# TODO update the JSON string below
json = "{}"
# create an instance of GraphCompareSignal from a JSON string
graph_compare_signal_instance = GraphCompareSignal.from_json(json)
# print the JSON string representation of the object
print(GraphCompareSignal.to_json())

# convert the object into a dict
graph_compare_signal_dict = graph_compare_signal_instance.to_dict()
# create an instance of GraphCompareSignal from a dict
graph_compare_signal_from_dict = GraphCompareSignal.from_dict(graph_compare_signal_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


