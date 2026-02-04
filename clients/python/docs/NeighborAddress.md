# NeighborAddress

Neighbor address model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**value** | [**Values**](Values.md) |  | 
**no_txs** | **int** |  | 
**address** | [**Address**](Address.md) |  | 
**labels** | **List[str]** |  | [optional] 
**token_values** | [**Dict[str, Values]**](Values.md) |  | [optional] 

## Example

```python
from graphsense.models.neighbor_address import NeighborAddress

# TODO update the JSON string below
json = "{}"
# create an instance of NeighborAddress from a JSON string
neighbor_address_instance = NeighborAddress.from_json(json)
# print the JSON string representation of the object
print(NeighborAddress.to_json())

# convert the object into a dict
neighbor_address_dict = neighbor_address_instance.to_dict()
# create an instance of NeighborAddress from a dict
neighbor_address_from_dict = NeighborAddress.from_dict(neighbor_address_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


