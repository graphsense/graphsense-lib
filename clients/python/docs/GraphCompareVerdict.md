# GraphCompareVerdict

Aggregator's opinion. Sub-verdicts kept independent.  Only the categorical tier (``relation``) is exposed. The internal aggregator also computes a numeric ``confidence`` and ``score_total`` (see ``ComparisonVerdictInternal``), but their weights have not been calibrated against ground-truth data, so they stay backend-only — consumers would inevitably treat them as probabilities. Add them here once calibrated.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**relation** | **str** |  | 
**cluster_verdict** | **str** |  | 
**discriminator_hits** | **List[str]** |  | [optional] 
**notes** | **List[str]** |  | [optional] 

## Example

```python
from graphsense.models.graph_compare_verdict import GraphCompareVerdict

# TODO update the JSON string below
json = "{}"
# create an instance of GraphCompareVerdict from a JSON string
graph_compare_verdict_instance = GraphCompareVerdict.from_json(json)
# print the JSON string representation of the object
print(GraphCompareVerdict.to_json())

# convert the object into a dict
graph_compare_verdict_dict = graph_compare_verdict_instance.to_dict()
# create an instance of GraphCompareVerdict from a dict
graph_compare_verdict_from_dict = GraphCompareVerdict.from_dict(graph_compare_verdict_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


