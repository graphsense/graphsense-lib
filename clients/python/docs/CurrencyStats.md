# CurrencyStats

Currency statistics model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**no_blocks** | **int** |  | 
**no_address_relations** | **int** |  | 
**no_addresses** | **int** |  | 
**no_entities** | **int** |  | 
**no_txs** | **int** |  | 
**no_labels** | **int** |  | 
**no_tagged_addresses** | **int** |  | 
**timestamp** | **int** |  | 
**network_type** | **str** |  | 

## Example

```python
from graphsense.models.currency_stats import CurrencyStats

# TODO update the JSON string below
json = "{}"
# create an instance of CurrencyStats from a JSON string
currency_stats_instance = CurrencyStats.from_json(json)
# print the JSON string representation of the object
print(CurrencyStats.to_json())

# convert the object into a dict
currency_stats_dict = currency_stats_instance.to_dict()
# create an instance of CurrencyStats from a dict
currency_stats_from_dict = CurrencyStats.from_dict(currency_stats_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


