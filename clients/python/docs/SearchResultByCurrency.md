# SearchResultByCurrency

Search result by currency.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**currency** | **str** |  |
**addresses** | **List[str]** |  |
**txs** | **List[str]** |  |

## Example

```python
from graphsense.models.search_result_by_currency import SearchResultByCurrency

# TODO update the JSON string below
json = "{}"
# create an instance of SearchResultByCurrency from a JSON string
search_result_by_currency_instance = SearchResultByCurrency.from_json(json)
# print the JSON string representation of the object
print(SearchResultByCurrency.to_json())

# convert the object into a dict
search_result_by_currency_dict = search_result_by_currency_instance.to_dict()
# create an instance of SearchResultByCurrency from a dict
search_result_by_currency_from_dict = SearchResultByCurrency.from_dict(search_result_by_currency_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
