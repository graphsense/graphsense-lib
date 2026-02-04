# TxSummary

Transaction summary model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**height** | **int** |  |
**timestamp** | **int** |  |
**tx_hash** | **str** |  |

## Example

```python
from graphsense.models.tx_summary import TxSummary

# TODO update the JSON string below
json = "{}"
# create an instance of TxSummary from a JSON string
tx_summary_instance = TxSummary.from_json(json)
# print the JSON string representation of the object
print(TxSummary.to_json())

# convert the object into a dict
tx_summary_dict = tx_summary_instance.to_dict()
# create an instance of TxSummary from a dict
tx_summary_from_dict = TxSummary.from_dict(tx_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
