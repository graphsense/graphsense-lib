# JoinMarketHeuristic


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**detected** | **bool** |  | 
**confidence** | **int** |  | 
**n_participants** | **int** |  | 
**denomination_sat** | **int** |  | 

## Example

```python
from graphsense.models.join_market_heuristic import JoinMarketHeuristic

# TODO update the JSON string below
json = "{}"
# create an instance of JoinMarketHeuristic from a JSON string
join_market_heuristic_instance = JoinMarketHeuristic.from_json(json)
# print the JSON string representation of the object
print(JoinMarketHeuristic.to_json())

# convert the object into a dict
join_market_heuristic_dict = join_market_heuristic_instance.to_dict()
# create an instance of JoinMarketHeuristic from a dict
join_market_heuristic_from_dict = JoinMarketHeuristic.from_dict(join_market_heuristic_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


