# GraphTxOverall

Network-agnostic rollup over all transactions in the set: fiat and timestamps only, since base units and block heights are not comparable across chains. Per-network notes carry their network as prefix.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_count** | **int** |  | 
**total_value_fiat** | [**List[Rate]**](Rate.md) |  | [optional] 
**timestamp_min** | **int** |  | 
**timestamp_max** | **int** |  | 
**notes** | **List[str]** |  | [optional] 

## Example

```python
from graphsense.models.graph_tx_overall import GraphTxOverall

# TODO update the JSON string below
json = "{}"
# create an instance of GraphTxOverall from a JSON string
graph_tx_overall_instance = GraphTxOverall.from_json(json)
# print the JSON string representation of the object
print(GraphTxOverall.to_json())

# convert the object into a dict
graph_tx_overall_dict = graph_tx_overall_instance.to_dict()
# create an instance of GraphTxOverall from a dict
graph_tx_overall_from_dict = GraphTxOverall.from_dict(graph_tx_overall_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


