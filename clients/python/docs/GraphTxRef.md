# GraphTxRef

A transaction reference: hash plus the network it lives on.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_hash** | **str** |  | 
**network** | **str** |  | 

## Example

```python
from graphsense.models.graph_tx_ref import GraphTxRef

# TODO update the JSON string below
json = "{}"
# create an instance of GraphTxRef from a JSON string
graph_tx_ref_instance = GraphTxRef.from_json(json)
# print the JSON string representation of the object
print(GraphTxRef.to_json())

# convert the object into a dict
graph_tx_ref_dict = graph_tx_ref_instance.to_dict()
# create an instance of GraphTxRef from a dict
graph_tx_ref_from_dict = GraphTxRef.from_dict(graph_tx_ref_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


