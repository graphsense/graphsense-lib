# CoinJoinConsensus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**detected** | **bool** |  | 
**confidence** | **int** |  | 
**sources** | **List[str]** |  | 

## Example

```python
from graphsense.models.coin_join_consensus import CoinJoinConsensus

# TODO update the JSON string below
json = "{}"
# create an instance of CoinJoinConsensus from a JSON string
coin_join_consensus_instance = CoinJoinConsensus.from_json(json)
# print the JSON string representation of the object
print(CoinJoinConsensus.to_json())

# convert the object into a dict
coin_join_consensus_dict = coin_join_consensus_instance.to_dict()
# create an instance of CoinJoinConsensus from a dict
coin_join_consensus_from_dict = CoinJoinConsensus.from_dict(coin_join_consensus_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


