# UserTagReportResponse

Response for user tag report submission.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 

## Example

```python
from graphsense.models.user_tag_report_response import UserTagReportResponse

# TODO update the JSON string below
json = "{}"
# create an instance of UserTagReportResponse from a JSON string
user_tag_report_response_instance = UserTagReportResponse.from_json(json)
# print the JSON string representation of the object
print(UserTagReportResponse.to_json())

# convert the object into a dict
user_tag_report_response_dict = user_tag_report_response_instance.to_dict()
# create an instance of UserTagReportResponse from a dict
user_tag_report_response_from_dict = UserTagReportResponse.from_dict(user_tag_report_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


