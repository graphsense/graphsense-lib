# GraphAddressNetworkSummary

Aggregate stats over one network's addresses. Value totals follow the ``Values`` pattern (native base unit plus per-code fiat sums); token holdings are excluded from native totals (noted).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**network** | **str** |  | 
**address_count** | **int** |  | 
**total_received** | [**Values**](Values.md) |  | 
**total_spent** | [**Values**](Values.md) |  | 
**balance** | [**Values**](Values.md) |  | 
**first_usage** | **int** |  | [optional] 
**last_usage** | **int** |  | [optional] 
**tagged_address_count** | **int** |  | [optional] [default to 0]
**actors** | [**List[LabeledItemRef]**](LabeledItemRef.md) |  | [optional] 
**notes** | [**List[GraphNote]**](GraphNote.md) |  | [optional] 

## Example

```python
from graphsense.models.graph_address_network_summary import GraphAddressNetworkSummary

# TODO update the JSON string below
json = "{}"
# create an instance of GraphAddressNetworkSummary from a JSON string
graph_address_network_summary_instance = GraphAddressNetworkSummary.from_json(json)
# print the JSON string representation of the object
print(GraphAddressNetworkSummary.to_json())

# convert the object into a dict
graph_address_network_summary_dict = graph_address_network_summary_instance.to_dict()
# create an instance of GraphAddressNetworkSummary from a dict
graph_address_network_summary_from_dict = GraphAddressNetworkSummary.from_dict(graph_address_network_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


