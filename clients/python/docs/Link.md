# Link


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
from graphsense.models.link import Link

# TODO update the JSON string below
json = "{}"
# create an instance of Link from a JSON string
link_instance = Link.from_json(json)
# print the JSON string representation of the object
print(Link.to_json())

# convert the object into a dict
link_dict = link_instance.to_dict()
# create an instance of Link from a dict
link_from_dict = Link.from_dict(link_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


