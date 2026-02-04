# ExternalConversion

External conversion (DEX swap or bridge) model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**conversion_type** | **str** |  | 
**from_address** | **str** |  | 
**to_address** | **str** |  | 
**from_asset** | **str** |  | 
**to_asset** | **str** |  | 
**from_amount** | **str** |  | 
**to_amount** | **str** |  | 
**from_asset_transfer** | **str** |  | 
**to_asset_transfer** | **str** |  | 
**from_network** | **str** |  | 
**to_network** | **str** |  | 
**from_is_supported_asset** | **bool** |  | 
**to_is_supported_asset** | **bool** |  | 

## Example

```python
from graphsense.models.external_conversion import ExternalConversion

# TODO update the JSON string below
json = "{}"
# create an instance of ExternalConversion from a JSON string
external_conversion_instance = ExternalConversion.from_json(json)
# print the JSON string representation of the object
print(ExternalConversion.to_json())

# convert the object into a dict
external_conversion_dict = external_conversion_instance.to_dict()
# create an instance of ExternalConversion from a dict
external_conversion_from_dict = ExternalConversion.from_dict(external_conversion_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


