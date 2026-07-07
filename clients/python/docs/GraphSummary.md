# GraphSummary

Aggregate stats over a graph node set, split by node type. Each block is present iff the request carried that node type.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**txs** | [**GraphTxSummary**](GraphTxSummary.md) |  | [optional] 
**addresses** | [**GraphAddressSummary**](GraphAddressSummary.md) |  | [optional] 

## Example

```python
from graphsense.models.graph_summary import GraphSummary

# TODO update the JSON string below
json = "{}"
# create an instance of GraphSummary from a JSON string
graph_summary_instance = GraphSummary.from_json(json)
# print the JSON string representation of the object
print(GraphSummary.to_json())

# convert the object into a dict
graph_summary_dict = graph_summary_instance.to_dict()
# create an instance of GraphSummary from a dict
graph_summary_from_dict = GraphSummary.from_dict(graph_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


