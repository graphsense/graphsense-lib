# LabelSummary

Label summary model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**label** | **str** |  | 
**count** | **int** |  | 
**confidence** | **float** |  | 
**relevance** | **float** |  | 
**creators** | **List[str]** |  | 
**sources** | **List[str]** |  | 
**concepts** | **List[str]** |  | 
**lastmod** | **int** |  | 
**inherited_from** | **str** |  | [optional] 

## Example

```python
from graphsense.models.label_summary import LabelSummary

# TODO update the JSON string below
json = "{}"
# create an instance of LabelSummary from a JSON string
label_summary_instance = LabelSummary.from_json(json)
# print the JSON string representation of the object
print(LabelSummary.to_json())

# convert the object into a dict
label_summary_dict = label_summary_instance.to_dict()
# create an instance of LabelSummary from a dict
label_summary_from_dict = LabelSummary.from_dict(label_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


