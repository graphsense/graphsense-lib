# ComparisonVerdict

Aggregator's opinion. Sub-verdicts kept independent.  ``confidence`` and ``score_total`` are tentative, weights have not yet been calibrated against ground-truth data.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**relation** | **str** |  | 
**confidence** | **int** |  | 
**cluster_verdict** | **str** |  | 
**discriminator_hits** | **List[str]** |  | [optional] 
**score_total** | **float** |  | [optional] [default to 0.0]
**notes** | **List[str]** |  | [optional] 

## Example

```python
from graphsense.models.comparison_verdict import ComparisonVerdict

# TODO update the JSON string below
json = "{}"
# create an instance of ComparisonVerdict from a JSON string
comparison_verdict_instance = ComparisonVerdict.from_json(json)
# print the JSON string representation of the object
print(ComparisonVerdict.to_json())

# convert the object into a dict
comparison_verdict_dict = comparison_verdict_instance.to_dict()
# create an instance of ComparisonVerdict from a dict
comparison_verdict_from_dict = ComparisonVerdict.from_dict(comparison_verdict_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


