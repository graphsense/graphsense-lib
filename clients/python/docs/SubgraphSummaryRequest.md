# SubgraphSummaryRequest

Request body for ``POST /{currency}/graph/summary``.  The subgraph is defined by ``txs`` (transaction hashes) and/or ``addresses``. Each non-empty list must hold at least 2 distinct entries; together they may hold at most 100. ``fiat_currency`` selects the currency for the fiat totals (only the rates GraphSense stores, usd and eur, are available; default usd).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**txs** | **List[str]** |  | [optional] 
**addresses** | **List[str]** |  | [optional] 
**fiat_currency** | **str** |  | [optional] [default to 'usd']

## Example

```python
from graphsense.models.subgraph_summary_request import SubgraphSummaryRequest

# TODO update the JSON string below
json = "{}"
# create an instance of SubgraphSummaryRequest from a JSON string
subgraph_summary_request_instance = SubgraphSummaryRequest.from_json(json)
# print the JSON string representation of the object
print(SubgraphSummaryRequest.to_json())

# convert the object into a dict
subgraph_summary_request_dict = subgraph_summary_request_instance.to_dict()
# create an instance of SubgraphSummaryRequest from a dict
subgraph_summary_request_from_dict = SubgraphSummaryRequest.from_dict(subgraph_summary_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


