# SearchResultLevel3

Search result at depth 3.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbor** | [**NeighborEntity**](NeighborEntity.md) |  | 
**matching_addresses** | [**List[Address]**](Address.md) |  | 
**paths** | [**List[SearchResultLevel4]**](SearchResultLevel4.md) |  | 

## Example

```python
from graphsense.models.search_result_level3 import SearchResultLevel3

# TODO update the JSON string below
json = "{}"
# create an instance of SearchResultLevel3 from a JSON string
search_result_level3_instance = SearchResultLevel3.from_json(json)
# print the JSON string representation of the object
print(SearchResultLevel3.to_json())

# convert the object into a dict
search_result_level3_dict = search_result_level3_instance.to_dict()
# create an instance of SearchResultLevel3 from a dict
search_result_level3_from_dict = SearchResultLevel3.from_dict(search_result_level3_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


