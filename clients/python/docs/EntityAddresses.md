# EntityAddresses

Paginated list of addresses in an entity.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**addresses** | [**List[Address]**](Address.md) |  | 
**next_page** | **str** |  | [optional] 

## Example

```python
from graphsense.models.entity_addresses import EntityAddresses

# TODO update the JSON string below
json = "{}"
# create an instance of EntityAddresses from a JSON string
entity_addresses_instance = EntityAddresses.from_json(json)
# print the JSON string representation of the object
print(EntityAddresses.to_json())

# convert the object into a dict
entity_addresses_dict = entity_addresses_instance.to_dict()
# create an instance of EntityAddresses from a dict
entity_addresses_from_dict = EntityAddresses.from_dict(entity_addresses_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


