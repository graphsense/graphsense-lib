# AddressRef

An address reference: address plus the network it lives on.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address** | **str** |  | 
**network** | **str** |  | 

## Example

```python
from graphsense.models.address_ref import AddressRef

# TODO update the JSON string below
json = "{}"
# create an instance of AddressRef from a JSON string
address_ref_instance = AddressRef.from_json(json)
# print the JSON string representation of the object
print(AddressRef.to_json())

# convert the object into a dict
address_ref_dict = address_ref_instance.to_dict()
# create an instance of AddressRef from a dict
address_ref_from_dict = AddressRef.from_dict(address_ref_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


