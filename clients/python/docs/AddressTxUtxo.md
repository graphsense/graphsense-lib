# AddressTxUtxo

UTXO transaction for an address.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_type** | **str** |  | [optional] [default to 'utxo']
**tx_hash** | **str** |  |
**currency** | **str** |  |
**coinbase** | **bool** |  |
**height** | **int** |  |
**timestamp** | **int** |  |
**value** | [**Values**](Values.md) |  |

## Example

```python
from graphsense.models.address_tx_utxo import AddressTxUtxo

# TODO update the JSON string below
json = "{}"
# create an instance of AddressTxUtxo from a JSON string
address_tx_utxo_instance = AddressTxUtxo.from_json(json)
# print the JSON string representation of the object
print(AddressTxUtxo.to_json())

# convert the object into a dict
address_tx_utxo_dict = address_tx_utxo_instance.to_dict()
# create an instance of AddressTxUtxo from a dict
address_tx_utxo_from_dict = AddressTxUtxo.from_dict(address_tx_utxo_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
