# UserReportedTag

User reported tag model

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address** | **str** |  |
**network** | **str** |  |
**actor** | **str** |  | [optional]
**label** | **str** |  |
**description** | **str** |  | [optional]

## Example

```python
from graphsense.models.user_reported_tag import UserReportedTag

# TODO update the JSON string below
json = "{}"
# create an instance of UserReportedTag from a JSON string
user_reported_tag_instance = UserReportedTag.from_json(json)
# print the JSON string representation of the object
print(UserReportedTag.to_json())

# convert the object into a dict
user_reported_tag_dict = user_reported_tag_instance.to_dict()
# create an instance of UserReportedTag from a dict
user_reported_tag_from_dict = UserReportedTag.from_dict(user_reported_tag_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
