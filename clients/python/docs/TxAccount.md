# TxAccount

Account-based transaction model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_type** | **str** |  | [optional] [default to 'account']
**identifier** | **str** |  |
**currency** | **str** |  |
**network** | **str** |  |
**tx_hash** | **str** |  |
**height** | **int** |  |
**timestamp** | **int** |  |
**value** | [**Values**](Values.md) |  |
**from_address** | **str** |  |
**to_address** | **str** |  |
**token_tx_id** | **int** |  | [optional]
**fee** | [**Values**](Values.md) |  | [optional]
**contract_creation** | **bool** |  | [optional]
**is_external** | **bool** |  | [optional]

## Example

```python
from graphsense.models.tx_account import TxAccount

# TODO update the JSON string below
json = "{}"
# create an instance of TxAccount from a JSON string
tx_account_instance = TxAccount.from_json(json)
# print the JSON string representation of the object
print(TxAccount.to_json())

# convert the object into a dict
tx_account_dict = tx_account_instance.to_dict()
# create an instance of TxAccount from a dict
tx_account_from_dict = TxAccount.from_dict(tx_account_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
