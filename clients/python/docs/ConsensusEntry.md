# ConsensusEntry


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**output** | [**AddressOutput**](AddressOutput.md) |  | 
**confidence** | **int** |  | 
**sources** | **List[str]** |  | 

## Example

```python
from graphsense.models.consensus_entry import ConsensusEntry

# TODO update the JSON string below
json = "{}"
# create an instance of ConsensusEntry from a JSON string
consensus_entry_instance = ConsensusEntry.from_json(json)
# print the JSON string representation of the object
print(ConsensusEntry.to_json())

# convert the object into a dict
consensus_entry_dict = consensus_entry_instance.to_dict()
# create an instance of ConsensusEntry from a dict
consensus_entry_from_dict = ConsensusEntry.from_dict(consensus_entry_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


