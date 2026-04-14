# ClusterAddresses

Paginated list of addresses in a cluster (canonical name).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**addresses** | [**List[Address]**](Address.md) |  | 
**next_page** | **str** |  | [optional] 

## Example

```python
from graphsense.models.cluster_addresses import ClusterAddresses

# TODO update the JSON string below
json = "{}"
# create an instance of ClusterAddresses from a JSON string
cluster_addresses_instance = ClusterAddresses.from_json(json)
# print the JSON string representation of the object
print(ClusterAddresses.to_json())

# convert the object into a dict
cluster_addresses_dict = cluster_addresses_instance.to_dict()
# create an instance of ClusterAddresses from a dict
cluster_addresses_from_dict = ClusterAddresses.from_dict(cluster_addresses_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


