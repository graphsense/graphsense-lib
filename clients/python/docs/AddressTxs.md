# AddressTxs

Paginated list of address transactions.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address_txs** | [**List[AddressTx]**](AddressTx.md) |  |
**next_page** | **str** |  | [optional]

## Example

```python
from graphsense.models.address_txs import AddressTxs

# TODO update the JSON string below
json = "{}"
# create an instance of AddressTxs from a JSON string
address_txs_instance = AddressTxs.from_json(json)
# print the JSON string representation of the object
print(AddressTxs.to_json())

# convert the object into a dict
address_txs_dict = address_txs_instance.to_dict()
# create an instance of AddressTxs from a dict
address_txs_from_dict = AddressTxs.from_dict(address_txs_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
