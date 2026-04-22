# ChangeHeuristics


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**consensus** | [**List[ConsensusEntry]**](ConsensusEntry.md) |  | 
**one_time_change** | [**OneTimeChangeHeuristic**](OneTimeChangeHeuristic.md) |  | [optional] 
**direct_change** | [**DirectChangeHeuristic**](DirectChangeHeuristic.md) |  | [optional] 
**multi_input_change** | [**MultiInputChangeHeuristic**](MultiInputChangeHeuristic.md) |  | [optional] 

## Example

```python
from graphsense.models.change_heuristics import ChangeHeuristics

# TODO update the JSON string below
json = "{}"
# create an instance of ChangeHeuristics from a JSON string
change_heuristics_instance = ChangeHeuristics.from_json(json)
# print the JSON string representation of the object
print(ChangeHeuristics.to_json())

# convert the object into a dict
change_heuristics_dict = change_heuristics_instance.to_dict()
# create an instance of ChangeHeuristics from a dict
change_heuristics_from_dict = ChangeHeuristics.from_dict(change_heuristics_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


