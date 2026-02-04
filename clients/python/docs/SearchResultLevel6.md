# SearchResultLevel6

Search result at depth 6 (leaf).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbor** | [**NeighborEntity**](NeighborEntity.md) |  |
**matching_addresses** | [**List[Address]**](Address.md) |  |

## Example

```python
from graphsense.models.search_result_level6 import SearchResultLevel6

# TODO update the JSON string below
json = "{}"
# create an instance of SearchResultLevel6 from a JSON string
search_result_level6_instance = SearchResultLevel6.from_json(json)
# print the JSON string representation of the object
print(SearchResultLevel6.to_json())

# convert the object into a dict
search_result_level6_dict = search_result_level6_instance.to_dict()
# create an instance of SearchResultLevel6 from a dict
search_result_level6_from_dict = SearchResultLevel6.from_dict(search_result_level6_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
