# NeighborEntities

Paginated list of neighbor entities.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbors** | [**List[NeighborEntity]**](NeighborEntity.md) |  |
**next_page** | **str** |  | [optional]

## Example

```python
from graphsense.models.neighbor_entities import NeighborEntities

# TODO update the JSON string below
json = "{}"
# create an instance of NeighborEntities from a JSON string
neighbor_entities_instance = NeighborEntities.from_json(json)
# print the JSON string representation of the object
print(NeighborEntities.to_json())

# convert the object into a dict
neighbor_entities_dict = neighbor_entities_instance.to_dict()
# create an instance of NeighborEntities from a dict
neighbor_entities_from_dict = NeighborEntities.from_dict(neighbor_entities_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
