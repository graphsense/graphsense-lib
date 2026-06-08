# SubgraphSummary

Aggregate stats over a subgraph, split by node type.  ``txs`` summarizes the transactions in the subgraph. ``addresses`` is reserved for a future per-address summary block and is omitted until address inputs are supported.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**currency** | **str** |  | 
**txs** | [**SubgraphTxSummary**](SubgraphTxSummary.md) |  | 
**addresses** | **object** |  | [optional] 

## Example

```python
from graphsense.models.subgraph_summary import SubgraphSummary

# TODO update the JSON string below
json = "{}"
# create an instance of SubgraphSummary from a JSON string
subgraph_summary_instance = SubgraphSummary.from_json(json)
# print the JSON string representation of the object
print(SubgraphSummary.to_json())

# convert the object into a dict
subgraph_summary_dict = subgraph_summary_instance.to_dict()
# create an instance of SubgraphSummary from a dict
subgraph_summary_from_dict = SubgraphSummary.from_dict(subgraph_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


