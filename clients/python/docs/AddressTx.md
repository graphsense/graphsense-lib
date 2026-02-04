# AddressTx


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_type** | **str** |  | [optional] [default to 'account']
**tx_hash** | **str** |  | 
**currency** | **str** |  | 
**coinbase** | **bool** |  | 
**height** | **int** |  | 
**timestamp** | **int** |  | 
**value** | [**Values**](Values.md) |  | 
**identifier** | **str** |  | 
**network** | **str** |  | 
**from_address** | **str** |  | 
**to_address** | **str** |  | 
**token_tx_id** | **int** |  | [optional] 
**fee** | [**Values**](Values.md) |  | [optional] 
**contract_creation** | **bool** |  | [optional] 
**is_external** | **bool** |  | [optional] 

## Example

```python
from graphsense.models.address_tx import AddressTx

# TODO update the JSON string below
json = "{}"
# create an instance of AddressTx from a JSON string
address_tx_instance = AddressTx.from_json(json)
# print the JSON string representation of the object
print(AddressTx.to_json())

# convert the object into a dict
address_tx_dict = address_tx_instance.to_dict()
# create an instance of AddressTx from a dict
address_tx_from_dict = AddressTx.from_dict(address_tx_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


