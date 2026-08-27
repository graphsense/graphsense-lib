# NetworkCapabilities

Feature availability of one network in this deployment.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**network** | **str** |  | 
**disabled** | **List[str]** | Feature families NOT served for this network, from the vocabulary \&quot;relations\&quot; (neighbors/links counterparty enumeration), \&quot;clusters\&quot; (address clustering), \&quot;tags\&quot; (TagStore data), \&quot;conversions\&quot; (DEX-swap/bridge resolution on txs), \&quot;exact_stats\&quot; (the per-currency /stats numbers are placeholders, not pipeline-exact). Routes of a disabled family answer 501. Unknown flags must be ignored (forward-extensible). Core route families (address/tx/block detail, search, rates) are never listed here — they work on every served network. | 

## Example

```python
from graphsense.models.network_capabilities import NetworkCapabilities

# TODO update the JSON string below
json = "{}"
# create an instance of NetworkCapabilities from a JSON string
network_capabilities_instance = NetworkCapabilities.from_json(json)
# print the JSON string representation of the object
print(NetworkCapabilities.to_json())

# convert the object into a dict
network_capabilities_dict = network_capabilities_instance.to_dict()
# create an instance of NetworkCapabilities from a dict
network_capabilities_from_dict = NetworkCapabilities.from_dict(network_capabilities_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


