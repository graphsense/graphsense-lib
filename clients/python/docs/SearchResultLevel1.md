# SearchResultLevel1

Search result at depth 1.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**neighbor** | [**NeighborEntity**](NeighborEntity.md) |  |
**matching_addresses** | [**List[Address]**](Address.md) |  |
**paths** | [**List[SearchResultLevel2]**](SearchResultLevel2.md) |  |

## Example

```python
from graphsense.models.search_result_level1 import SearchResultLevel1

# TODO update the JSON string below
json = "{}"
# create an instance of SearchResultLevel1 from a JSON string
search_result_level1_instance = SearchResultLevel1.from_json(json)
# print the JSON string representation of the object
print(SearchResultLevel1.to_json())

# convert the object into a dict
search_result_level1_dict = search_result_level1_instance.to_dict()
# create an instance of SearchResultLevel1 from a dict
search_result_level1_from_dict = SearchResultLevel1.from_dict(search_result_level1_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
