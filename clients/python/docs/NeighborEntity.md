# NeighborEntity

Neighbor cluster model (legacy name: NeighborEntity).  Dual-emits the neighbor reference under both `entity` (deprecated) and `cluster` (preferred) keys, mirroring the alias pattern on the top-level `Entity`/`Cluster` models. The value is either an integer ID or a full `Entity`/`Cluster` object; whichever shape `entity` carries, `cluster` carries the same value.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**value** | [**Values**](Values.md) |  | 
**no_txs** | **int** |  | 
**entity** | [**Entity**](Entity.md) |  | [optional] 
**labels** | **List[str]** |  | [optional] 
**token_values** | [**Dict[str, Values]**](Values.md) |  | [optional] 
**cluster** | [**Cluster**](Cluster.md) |  | 

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


