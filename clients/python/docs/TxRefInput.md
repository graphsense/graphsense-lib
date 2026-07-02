# TxRefInput

A transaction reference: hash plus the network it lives on.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_hash** | **str** |  | 
**network** | **str** |  | 

## Example

```python
from graphsense.models.tx_ref_input import TxRefInput

# TODO update the JSON string below
json = "{}"
# create an instance of TxRefInput from a JSON string
tx_ref_input_instance = TxRefInput.from_json(json)
# print the JSON string representation of the object
print(TxRefInput.to_json())

# convert the object into a dict
tx_ref_input_dict = tx_ref_input_instance.to_dict()
# create an instance of TxRefInput from a dict
tx_ref_input_from_dict = TxRefInput.from_dict(tx_ref_input_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


