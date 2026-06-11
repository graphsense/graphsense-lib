# ComparisonSignal

One row of the pairwise comparison table; values stringified per tx.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**kind** | **str** |  | 
**per_tx** | **List[Optional[str]]** |  | 
**verdict** | **str** |  | 
**weight** | **int** |  | [optional] [default to 0]

## Example

```python
from graphsense.models.comparison_signal import ComparisonSignal

# TODO update the JSON string below
json = "{}"
# create an instance of ComparisonSignal from a JSON string
comparison_signal_instance = ComparisonSignal.from_json(json)
# print the JSON string representation of the object
print(ComparisonSignal.to_json())

# convert the object into a dict
comparison_signal_dict = comparison_signal_instance.to_dict()
# create an instance of ComparisonSignal from a dict
comparison_signal_from_dict = ComparisonSignal.from_dict(comparison_signal_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


