# BlockAtDate

Block at date model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**before_block** | **int** |  | [optional] 
**before_timestamp** | **int** |  | [optional] 
**after_block** | **int** |  | [optional] 
**after_timestamp** | **int** |  | [optional] 

## Example

```python
from graphsense.models.block_at_date import BlockAtDate

# TODO update the JSON string below
json = "{}"
# create an instance of BlockAtDate from a JSON string
block_at_date_instance = BlockAtDate.from_json(json)
# print the JSON string representation of the object
print(BlockAtDate.to_json())

# convert the object into a dict
block_at_date_dict = block_at_date_instance.to_dict()
# create an instance of BlockAtDate from a dict
block_at_date_from_dict = BlockAtDate.from_dict(block_at_date_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


