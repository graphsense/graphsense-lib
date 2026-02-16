# LinkUtxo

UTXO link model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_type** | **str** |  | [optional] [default to 'utxo']
**tx_hash** | **str** |  | 
**currency** | **str** |  | 
**height** | **int** |  | 
**timestamp** | **int** |  | 
**input_value** | [**Values**](Values.md) |  | 
**output_value** | [**Values**](Values.md) |  | 

## Example

```python
from graphsense.models.link_utxo import LinkUtxo

# TODO update the JSON string below
json = "{}"
# create an instance of LinkUtxo from a JSON string
link_utxo_instance = LinkUtxo.from_json(json)
# print the JSON string representation of the object
print(LinkUtxo.to_json())

# convert the object into a dict
link_utxo_dict = link_utxo_instance.to_dict()
# create an instance of LinkUtxo from a dict
link_utxo_from_dict = LinkUtxo.from_dict(link_utxo_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


