# GraphComparedTx

Per-tx entry. ``characteristics`` and ``details`` are populated iff the request's ``include`` list names them (``details`` is off by default).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_hash** | **str** |  | 
**network** | **str** |  | 
**characteristics** | [**GraphTxCharacteristics**](GraphTxCharacteristics.md) |  | [optional] 
**details** | [**Tx**](Tx.md) |  | [optional] 

## Example

```python
from graphsense.models.graph_compared_tx import GraphComparedTx

# TODO update the JSON string below
json = "{}"
# create an instance of GraphComparedTx from a JSON string
graph_compared_tx_instance = GraphComparedTx.from_json(json)
# print the JSON string representation of the object
print(GraphComparedTx.to_json())

# convert the object into a dict
graph_compared_tx_dict = graph_compared_tx_instance.to_dict()
# create an instance of GraphComparedTx from a dict
graph_compared_tx_from_dict = GraphComparedTx.from_dict(graph_compared_tx_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


