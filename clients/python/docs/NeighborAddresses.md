# NeighborAddresses

Paginated list of neighbor addresses.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbors** | [**List[NeighborAddress]**](NeighborAddress.md) |  |
**next_page** | **str** |  | [optional]

## Example

```python
from graphsense.models.neighbor_addresses import NeighborAddresses

# TODO update the JSON string below
json = "{}"
# create an instance of NeighborAddresses from a JSON string
neighbor_addresses_instance = NeighborAddresses.from_json(json)
# print the JSON string representation of the object
print(NeighborAddresses.to_json())

# convert the object into a dict
neighbor_addresses_dict = neighbor_addresses_instance.to_dict()
# create an instance of NeighborAddresses from a dict
neighbor_addresses_from_dict = NeighborAddresses.from_dict(neighbor_addresses_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
