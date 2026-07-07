# GraphAddressSummary


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**overall** | [**GraphAddressOverall**](GraphAddressOverall.md) |  | 
**networks** | [**List[GraphAddressNetworkSummary]**](GraphAddressNetworkSummary.md) |  | 

## Example

```python
from graphsense.models.graph_address_summary import GraphAddressSummary

# TODO update the JSON string below
json = "{}"
# create an instance of GraphAddressSummary from a JSON string
graph_address_summary_instance = GraphAddressSummary.from_json(json)
# print the JSON string representation of the object
print(GraphAddressSummary.to_json())

# convert the object into a dict
graph_address_summary_dict = graph_address_summary_instance.to_dict()
# create an instance of GraphAddressSummary from a dict
graph_address_summary_from_dict = GraphAddressSummary.from_dict(graph_address_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


