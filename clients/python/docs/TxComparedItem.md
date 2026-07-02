# TxComparedItem

Per-tx entry. ``characteristics`` is populated when ``include_characteristics`` is set (default true), ``details`` when ``include_details`` is set.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_hash** | **str** |  | 
**network** | **str** |  | [optional] [default to 'btc']
**characteristics** | [**TxCharacteristics**](TxCharacteristics.md) |  | [optional] 
**details** | [**Tx**](Tx.md) |  | [optional] 

## Example

```python
from graphsense.models.tx_compared_item import TxComparedItem

# TODO update the JSON string below
json = "{}"
# create an instance of TxComparedItem from a JSON string
tx_compared_item_instance = TxComparedItem.from_json(json)
# print the JSON string representation of the object
print(TxComparedItem.to_json())

# convert the object into a dict
tx_compared_item_dict = tx_compared_item_instance.to_dict()
# create an instance of TxComparedItem from a dict
tx_compared_item_from_dict = TxComparedItem.from_dict(tx_compared_item_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


