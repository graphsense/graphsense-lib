# ComparisonSummary

Aggregate stats over all compared transactions.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_count** | **int** |  | 
**currency** | **str** |  | 
**total_output_sat** | **int** |  | 
**total_inputs** | **int** |  | 
**total_outputs** | **int** |  | 
**block_min** | **int** |  | 
**block_max** | **int** |  | 
**timestamp_min** | **int** |  | 
**timestamp_max** | **int** |  | 

## Example

```python
from graphsense.models.comparison_summary import ComparisonSummary

# TODO update the JSON string below
json = "{}"
# create an instance of ComparisonSummary from a JSON string
comparison_summary_instance = ComparisonSummary.from_json(json)
# print the JSON string representation of the object
print(ComparisonSummary.to_json())

# convert the object into a dict
comparison_summary_dict = comparison_summary_instance.to_dict()
# create an instance of ComparisonSummary from a dict
comparison_summary_from_dict = ComparisonSummary.from_dict(comparison_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


