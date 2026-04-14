# NeighborClusters

Paginated list of neighbor clusters (canonical name).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbors** | [**List[NeighborCluster]**](NeighborCluster.md) |  | 
**next_page** | **str** |  | [optional] 

## Example

```python
from graphsense.models.neighbor_clusters import NeighborClusters

# TODO update the JSON string below
json = "{}"
# create an instance of NeighborClusters from a JSON string
neighbor_clusters_instance = NeighborClusters.from_json(json)
# print the JSON string representation of the object
print(NeighborClusters.to_json())

# convert the object into a dict
neighbor_clusters_dict = neighbor_clusters_instance.to_dict()
# create an instance of NeighborClusters from a dict
neighbor_clusters_from_dict = NeighborClusters.from_dict(neighbor_clusters_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


