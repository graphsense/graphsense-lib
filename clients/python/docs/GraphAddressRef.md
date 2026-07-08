# GraphAddressRef

An address reference: address plus the network it lives on.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address** | **str** |  | 
**network** | **str** |  | 

## Example

```python
from graphsense.models.graph_address_ref import GraphAddressRef

# TODO update the JSON string below
json = "{}"
# create an instance of GraphAddressRef from a JSON string
graph_address_ref_instance = GraphAddressRef.from_json(json)
# print the JSON string representation of the object
print(GraphAddressRef.to_json())

# convert the object into a dict
graph_address_ref_dict = graph_address_ref_instance.to_dict()
# create an instance of GraphAddressRef from a dict
graph_address_ref_from_dict = GraphAddressRef.from_dict(graph_address_ref_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


