# TxRef

Transaction reference model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**input_index** | **int** |  |
**output_index** | **int** |  |
**tx_hash** | **str** |  |

## Example

```python
from graphsense.models.tx_ref import TxRef

# TODO update the JSON string below
json = "{}"
# create an instance of TxRef from a JSON string
tx_ref_instance = TxRef.from_json(json)
# print the JSON string representation of the object
print(TxRef.to_json())

# convert the object into a dict
tx_ref_dict = tx_ref_instance.to_dict()
# create an instance of TxRef from a dict
tx_ref_from_dict = TxRef.from_dict(tx_ref_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
