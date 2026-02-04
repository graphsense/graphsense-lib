# AddressTag

Address tag model with address-specific fields.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**label** | **str** |  | [optional]
**tag_type** | **str** |  | [optional]
**tagpack_title** | **str** |  | [optional]
**tagpack_is_public** | **bool** |  | [optional]
**tagpack_creator** | **str** |  | [optional]
**is_cluster_definer** | **bool** |  | [optional]
**currency** | **str** |  | [optional]
**category** | **str** |  | [optional]
**concepts** | **List[str]** |  | [optional]
**actor** | **str** |  | [optional]
**abuse** | **str** |  | [optional]
**tagpack_uri** | **str** |  | [optional]
**source** | **str** |  | [optional]
**lastmod** | **int** |  | [optional]
**confidence** | **str** |  | [optional]
**confidence_level** | **int** |  | [optional]
**inherited_from** | **str** |  | [optional]
**address** | **str** |  | [optional]
**entity** | **int** |  | [optional]

## Example

```python
from graphsense.models.address_tag import AddressTag

# TODO update the JSON string below
json = "{}"
# create an instance of AddressTag from a JSON string
address_tag_instance = AddressTag.from_json(json)
# print the JSON string representation of the object
print(AddressTag.to_json())

# convert the object into a dict
address_tag_dict = address_tag_instance.to_dict()
# create an instance of AddressTag from a dict
address_tag_from_dict = AddressTag.from_dict(address_tag_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
