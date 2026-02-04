# SearchResultLevel5

Search result at depth 5.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbor** | [**NeighborEntity**](NeighborEntity.md) |  |
**matching_addresses** | [**List[Address]**](Address.md) |  |
**paths** | [**List[SearchResultLevel6]**](SearchResultLevel6.md) |  |

## Example

```python
from graphsense.models.search_result_level5 import SearchResultLevel5

# TODO update the JSON string below
json = "{}"
# create an instance of SearchResultLevel5 from a JSON string
search_result_level5_instance = SearchResultLevel5.from_json(json)
# print the JSON string representation of the object
print(SearchResultLevel5.to_json())

# convert the object into a dict
search_result_level5_dict = search_result_level5_instance.to_dict()
# create an instance of SearchResultLevel5 from a dict
search_result_level5_from_dict = SearchResultLevel5.from_dict(search_result_level5_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
