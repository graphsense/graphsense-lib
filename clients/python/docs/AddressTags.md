# AddressTags

Paginated list of address tags.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address_tags** | [**List[AddressTag]**](AddressTag.md) |  | 
**next_page** | **str** |  | [optional] 

## Example

```python
from graphsense.models.address_tags import AddressTags

# TODO update the JSON string below
json = "{}"
# create an instance of AddressTags from a JSON string
address_tags_instance = AddressTags.from_json(json)
# print the JSON string representation of the object
print(AddressTags.to_json())

# convert the object into a dict
address_tags_dict = address_tags_instance.to_dict()
# create an instance of AddressTags from a dict
address_tags_from_dict = AddressTags.from_dict(address_tags_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


