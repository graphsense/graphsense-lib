# RelatedAddresses

Paginated list of related addresses.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**related_addresses** | [**List[RelatedAddress]**](RelatedAddress.md) |  | 
**next_page** | **str** |  | [optional] 

## Example

```python
from graphsense.models.related_addresses import RelatedAddresses

# TODO update the JSON string below
json = "{}"
# create an instance of RelatedAddresses from a JSON string
related_addresses_instance = RelatedAddresses.from_json(json)
# print the JSON string representation of the object
print(RelatedAddresses.to_json())

# convert the object into a dict
related_addresses_dict = related_addresses_instance.to_dict()
# create an instance of RelatedAddresses from a dict
related_addresses_from_dict = RelatedAddresses.from_dict(related_addresses_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


