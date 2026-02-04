# TxUtxo

UTXO transaction model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_type** | **str** |  | [optional] [default to 'utxo']
**currency** | **str** |  |
**tx_hash** | **str** |  |
**coinbase** | **bool** |  |
**height** | **int** |  |
**no_inputs** | **int** |  |
**no_outputs** | **int** |  |
**timestamp** | **int** |  |
**total_input** | [**Values**](Values.md) |  |
**total_output** | [**Values**](Values.md) |  |
**inputs** | [**List[TxValue]**](TxValue.md) |  | [optional]
**outputs** | [**List[TxValue]**](TxValue.md) |  | [optional]

## Example

```python
from graphsense.models.tx_utxo import TxUtxo

# TODO update the JSON string below
json = "{}"
# create an instance of TxUtxo from a JSON string
tx_utxo_instance = TxUtxo.from_json(json)
# print the JSON string representation of the object
print(TxUtxo.to_json())

# convert the object into a dict
tx_utxo_dict = tx_utxo_instance.to_dict()
# create an instance of TxUtxo from a dict
tx_utxo_from_dict = TxUtxo.from_dict(tx_utxo_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
