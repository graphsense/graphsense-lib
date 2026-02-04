# Tx


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_type** | **str** |  | [optional] [default to 'account']
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
**identifier** | **str** |  | 
**network** | **str** |  | 
**value** | [**Values**](Values.md) |  | 
**from_address** | **str** |  | 
**to_address** | **str** |  | 
**token_tx_id** | **int** |  | [optional] 
**fee** | [**Values**](Values.md) |  | [optional] 
**contract_creation** | **bool** |  | [optional] 
**is_external** | **bool** |  | [optional] 

## Example

```python
from graphsense.models.tx import Tx

# TODO update the JSON string below
json = "{}"
# create an instance of Tx from a JSON string
tx_instance = Tx.from_json(json)
# print the JSON string representation of the object
print(Tx.to_json())

# convert the object into a dict
tx_dict = tx_instance.to_dict()
# create an instance of Tx from a dict
tx_from_dict = Tx.from_dict(tx_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


