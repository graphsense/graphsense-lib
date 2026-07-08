# GraphComparison

Top-level response for /graph/compare.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**txs** | [**List[GraphComparedTx]**](GraphComparedTx.md) |  | 
**signals** | [**List[GraphCompareSignal]**](GraphCompareSignal.md) |  | [optional] 
**lineage** | [**List[GraphLineageEdge]**](GraphLineageEdge.md) |  | [optional] 
**verdict** | [**GraphCompareVerdict**](GraphCompareVerdict.md) |  | [optional] 

## Example

```python
from graphsense.models.graph_comparison import GraphComparison

# TODO update the JSON string below
json = "{}"
# create an instance of GraphComparison from a JSON string
graph_comparison_instance = GraphComparison.from_json(json)
# print the JSON string representation of the object
print(GraphComparison.to_json())

# convert the object into a dict
graph_comparison_dict = graph_comparison_instance.to_dict()
# create an instance of GraphComparison from a dict
graph_comparison_from_dict = GraphComparison.from_dict(graph_comparison_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


