# Address

Address model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**currency** | **str** |  |
**address** | **str** |  |
**entity** | **int** |  |
**balance** | [**Values**](Values.md) |  |
**total_received** | [**Values**](Values.md) |  |
**total_spent** | [**Values**](Values.md) |  |
**first_tx** | [**TxSummary**](TxSummary.md) |  |
**last_tx** | [**TxSummary**](TxSummary.md) |  |
**in_degree** | **int** |  |
**out_degree** | **int** |  |
**no_incoming_txs** | **int** |  |
**no_outgoing_txs** | **int** |  |
**token_balances** | [**Dict[str, Values]**](Values.md) |  | [optional]
**total_tokens_received** | [**Dict[str, Values]**](Values.md) |  | [optional]
**total_tokens_spent** | [**Dict[str, Values]**](Values.md) |  | [optional]
**actors** | [**List[LabeledItemRef]**](LabeledItemRef.md) |  | [optional]
**is_contract** | **bool** |  | [optional]
**status** | **str** |  | [optional]

## Example

```python
from graphsense.models.address import Address

# TODO update the JSON string below
json = "{}"
# create an instance of Address from a JSON string
address_instance = Address.from_json(json)
# print the JSON string representation of the object
print(Address.to_json())

# convert the object into a dict
address_dict = address_instance.to_dict()
# create an instance of Address from a dict
address_from_dict = Address.from_dict(address_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
