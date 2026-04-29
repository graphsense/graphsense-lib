# NeighborCluster

Neighbor cluster (canonical name, supersedes `NeighborEntity`).

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
from graphsense.models.neighbor_cluster import NeighborCluster

# TODO update the JSON string below
json = "{}"
# create an instance of NeighborCluster from a JSON string
neighbor_cluster_instance = NeighborCluster.from_json(json)
# print the JSON string representation of the object
print(NeighborCluster.to_json())

# convert the object into a dict
neighbor_cluster_dict = neighbor_cluster_instance.to_dict()
# create an instance of NeighborCluster from a dict
neighbor_cluster_from_dict = NeighborCluster.from_dict(neighbor_cluster_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


