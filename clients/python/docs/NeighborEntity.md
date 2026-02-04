# NeighborEntity

Neighbor entity model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**value** | [**Values**](Values.md) |  |
**no_txs** | **int** |  |
**entity** | [**Entity**](Entity.md) |  | [optional]
**labels** | **List[str]** |  | [optional]
**token_values** | [**Dict[str, Values]**](Values.md) |  | [optional]

## Example

```python
from graphsense.models.neighbor_entity import NeighborEntity

# TODO update the JSON string below
json = "{}"
# create an instance of NeighborEntity from a JSON string
neighbor_entity_instance = NeighborEntity.from_json(json)
# print the JSON string representation of the object
print(NeighborEntity.to_json())

# convert the object into a dict
neighbor_entity_dict = neighbor_entity_instance.to_dict()
# create an instance of NeighborEntity from a dict
neighbor_entity_from_dict = NeighborEntity.from_dict(neighbor_entity_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
