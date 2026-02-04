# SearchResultLevel4

Search result at depth 4.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbor** | [**NeighborEntity**](NeighborEntity.md) |  | 
**matching_addresses** | [**List[Address]**](Address.md) |  | 
**paths** | [**List[SearchResultLevel5]**](SearchResultLevel5.md) |  | 

## Example

```python
from graphsense.models.search_result_level4 import SearchResultLevel4

# TODO update the JSON string below
json = "{}"
# create an instance of SearchResultLevel4 from a JSON string
search_result_level4_instance = SearchResultLevel4.from_json(json)
# print the JSON string representation of the object
print(SearchResultLevel4.to_json())

# convert the object into a dict
search_result_level4_dict = search_result_level4_instance.to_dict()
# create an instance of SearchResultLevel4 from a dict
search_result_level4_from_dict = SearchResultLevel4.from_dict(search_result_level4_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


