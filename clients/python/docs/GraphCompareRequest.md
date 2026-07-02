# GraphCompareRequest

Request body for ``POST /graph/compare``.  The fingerprinting analysis is BTC-only for now; every ref's network must be ``btc`` (400 otherwise). ``include`` selects response components; signals, lineage and verdict are always computed internally (the verdict depends on the signals), the list only controls what is returned. ``all`` expands to every component.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**txs** | [**List[GraphTxRef]**](GraphTxRef.md) |  | 
**include** | **List[str]** |  | [optional] [default to ["characteristics","signals","lineage","verdict"]]

## Example

```python
from graphsense.models.graph_compare_request import GraphCompareRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GraphCompareRequest from a JSON string
graph_compare_request_instance = GraphCompareRequest.from_json(json)
# print the JSON string representation of the object
print(GraphCompareRequest.to_json())

# convert the object into a dict
graph_compare_request_dict = graph_compare_request_instance.to_dict()
# create an instance of GraphCompareRequest from a dict
graph_compare_request_from_dict = GraphCompareRequest.from_dict(graph_compare_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


