# LinksInner


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_type** | **str** |  | [optional] [default to 'account']
**tx_hash** | **str** |  | 
**currency** | **str** |  | 
**height** | **int** |  | 
**timestamp** | **int** |  | 
**input_value** | [**Values**](Values.md) |  | 
**output_value** | [**Values**](Values.md) |  | 
**coinbase** | **bool** |  | 
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
from graphsense.models.links_inner import LinksInner

# TODO update the JSON string below
json = "{}"
# create an instance of LinksInner from a JSON string
links_inner_instance = LinksInner.from_json(json)
# print the JSON string representation of the object
print(LinksInner.to_json())

# convert the object into a dict
links_inner_dict = links_inner_instance.to_dict()
# create an instance of LinksInner from a dict
links_inner_from_dict = LinksInner.from_dict(links_inner_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


