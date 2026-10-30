# AggregateCutoff

Per-field qualification of a truncated Address body served by an external GraphSense-compatible backend: consumers must be able to render a floor value as \"63,037+\" instead of presenting it as exact. Fields not listed in either list are exact. Local Cassandra serving computes exact aggregates and never emits this model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**floor_fields** | **List[str]** | Fields whose served value is a LOWER BOUND of the true value — the scan budget covered only part of the history. Render as \&quot;value+\&quot;. | 
**approximate_fields** | **List[str]** | Fields derived from the scanned sample that are neither exact nor a guaranteed bound (e.g. token_balances, a difference of two floors). | [optional] 

## Example

```python
from graphsense.models.aggregate_cutoff import AggregateCutoff

# TODO update the JSON string below
json = "{}"
# create an instance of AggregateCutoff from a JSON string
aggregate_cutoff_instance = AggregateCutoff.from_json(json)
# print the JSON string representation of the object
print(AggregateCutoff.to_json())

# convert the object into a dict
aggregate_cutoff_dict = aggregate_cutoff_instance.to_dict()
# create an instance of AggregateCutoff from a dict
aggregate_cutoff_from_dict = AggregateCutoff.from_dict(aggregate_cutoff_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


