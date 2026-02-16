# TxValue

Transaction value model for UTXO inputs/outputs.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address** | **List[str]** |  | 
**value** | [**Values**](Values.md) |  | 
**index** | **int** |  | [optional] 

## Example

```python
from graphsense.models.tx_value import TxValue

# TODO update the JSON string below
json = "{}"
# create an instance of TxValue from a JSON string
tx_value_instance = TxValue.from_json(json)
# print the JSON string representation of the object
print(TxValue.to_json())

# convert the object into a dict
tx_value_dict = tx_value_instance.to_dict()
# create an instance of TxValue from a dict
tx_value_from_dict = TxValue.from_dict(tx_value_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


