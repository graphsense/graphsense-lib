# SearchResultLevel2

Search result at depth 2.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbor** | [**NeighborEntity**](NeighborEntity.md) |  | 
**matching_addresses** | [**List[Address]**](Address.md) |  | 
**paths** | [**List[SearchResultLevel3]**](SearchResultLevel3.md) |  | 

## Example

```python
from graphsense.models.search_result_level2 import SearchResultLevel2

# TODO update the JSON string below
json = "{}"
# create an instance of SearchResultLevel2 from a JSON string
search_result_level2_instance = SearchResultLevel2.from_json(json)
# print the JSON string representation of the object
print(SearchResultLevel2.to_json())

# convert the object into a dict
search_result_level2_dict = search_result_level2_instance.to_dict()
# create an instance of SearchResultLevel2 from a dict
search_result_level2_from_dict = SearchResultLevel2.from_dict(search_result_level2_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


