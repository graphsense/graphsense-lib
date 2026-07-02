# GraphSummaryRequest

Request body for ``POST /graph/summary``.  The node set is defined by ``txs`` and/or ``addresses``; every item carries its own network, so the set may span chains. Each non-empty list must hold at least 2 distinct entries (keyed on network + hash); together they may hold at most 100. Fiat totals always carry every rate GraphSense stores (eur, usd).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**txs** | [**List[TxRefInput]**](TxRefInput.md) |  | [optional] 
**addresses** | [**List[AddressRef]**](AddressRef.md) |  | [optional] 

## Example

```python
from graphsense.models.graph_summary_request import GraphSummaryRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GraphSummaryRequest from a JSON string
graph_summary_request_instance = GraphSummaryRequest.from_json(json)
# print the JSON string representation of the object
print(GraphSummaryRequest.to_json())

# convert the object into a dict
graph_summary_request_dict = graph_summary_request_instance.to_dict()
# create an instance of GraphSummaryRequest from a dict
graph_summary_request_from_dict = GraphSummaryRequest.from_dict(graph_summary_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


