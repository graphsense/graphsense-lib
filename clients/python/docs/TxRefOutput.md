# TxRefOutput

Transaction reference model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**input_index** | **int** |  | 
**output_index** | **int** |  | 
**tx_hash** | **str** |  | 

## Example

```python
from graphsense.models.tx_ref_output import TxRefOutput

# TODO update the JSON string below
json = "{}"
# create an instance of TxRefOutput from a JSON string
tx_ref_output_instance = TxRefOutput.from_json(json)
# print the JSON string representation of the object
print(TxRefOutput.to_json())

# convert the object into a dict
tx_ref_output_dict = tx_ref_output_instance.to_dict()
# create an instance of TxRefOutput from a dict
tx_ref_output_from_dict = TxRefOutput.from_dict(tx_ref_output_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


