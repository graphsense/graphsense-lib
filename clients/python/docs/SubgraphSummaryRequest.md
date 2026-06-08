# SubgraphSummaryRequest

Request body for ``POST /{currency}/subgraph/summary``.  The subgraph is defined by ``txs`` (transaction hashes). ``addresses`` is reserved for a future extension and must be empty for now; the node set (txs + addresses) must hold at least 2 and at most 100 distinct nodes.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**txs** | **List[str]** |  | [optional] 
**addresses** | **List[str]** |  | [optional] 

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


