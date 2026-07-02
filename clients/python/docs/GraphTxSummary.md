# GraphTxSummary


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**overall** | [**GraphTxOverall**](GraphTxOverall.md) |  | 
**networks** | [**List[GraphTxNetworkSummary]**](GraphTxNetworkSummary.md) |  | 

## Example

```python
from graphsense.models.graph_tx_summary import GraphTxSummary

# TODO update the JSON string below
json = "{}"
# create an instance of GraphTxSummary from a JSON string
graph_tx_summary_instance = GraphTxSummary.from_json(json)
# print the JSON string representation of the object
print(GraphTxSummary.to_json())

# convert the object into a dict
graph_tx_summary_dict = graph_tx_summary_instance.to_dict()
# create an instance of GraphTxSummary from a dict
graph_tx_summary_from_dict = GraphTxSummary.from_dict(graph_tx_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


