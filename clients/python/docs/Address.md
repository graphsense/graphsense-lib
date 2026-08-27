# Address

Address model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**currency** | **str** |  | 
**address** | **str** |  | 
**entity** | **int** | Deprecated alias of &#x60;cluster&#x60;. Use &#x60;cluster&#x60; instead; this field is retained for backwards compatibility and will be removed in a future release. | 
**fresh_cluster_id** | **int** |  | [optional] 
**balance** | [**Values**](Values.md) |  | 
**total_received** | [**Values**](Values.md) |  | 
**total_spent** | [**Values**](Values.md) |  | 
**first_tx** | [**TxSummary**](TxSummary.md) | First transaction in which this address appears, over its entire history — independent of any neighbor, direction, or date filter. Null if the address has no transactions of its own. | [optional] 
**last_tx** | [**TxSummary**](TxSummary.md) | Last transaction in which this address appears, over its entire history — independent of any neighbor, direction, or date filter. Null if the address has no transactions of its own. | [optional] 
**in_degree** | **int** |  | 
**out_degree** | **int** |  | 
**no_incoming_txs** | **int** |  | 
**no_outgoing_txs** | **int** |  | 
**aggregates_truncated** | **bool** | True when count/degree and total fields are lower bounds because the backend could not cover the address&#39;s full history (provider-call budget, or flows invisible to the provider). Absent means values are computed over the full history. | [optional] 
**cutoff** | [**AggregateCutoff**](AggregateCutoff.md) | Present exactly when aggregates_truncated is true: names which fields are floors (render as \&quot;value+\&quot;) and which are sample-approximations. Absent means every served field is exact. | [optional] 
**token_balances** | [**Dict[str, Values]**](Values.md) |  | [optional] 
**total_tokens_received** | [**Dict[str, Values]**](Values.md) |  | [optional] 
**total_tokens_spent** | [**Dict[str, Values]**](Values.md) |  | [optional] 
**actors** | [**List[LabeledItemRef]**](LabeledItemRef.md) |  | [optional] 
**is_contract** | **bool** |  | [optional] 
**is_possible_service** | **bool** | Structural heuristic: True when the address is likely a service (exchange, payment processor, ...) judged from degree/tx counts (account networks) or its cluster&#39;s size and degrees (UTXO networks). Tag data is deliberately not consulted. Absent means the serving backend did not compute it (older server, embedded neighbor bodies) — consumers fall back to their own judgment. | [optional] 
**qualifiers** | **Dict[str, str]** | Flat per-field qualification map, the simple consumer form of cutoff: field name -&gt; \&quot;gt\&quot; (served value is a lower bound of the true value) or \&quot;approx\&quot; (neither exact nor a guaranteed bound). Absent means every served field is exact. Set only by external GraphSense-compatible backends with provider-call budgets; local Cassandra serving computes exact aggregates. | [optional] 
**status** | **str** | Legacy field. Do not use — retained only for backwards compatibility and will be removed in a future release. | [optional] 
**cluster** | **int** | Address cluster ID (preferred alias for the deprecated &#x60;entity&#x60; field). | [readonly] 

## Example

```python
from graphsense.models.address import Address

# TODO update the JSON string below
json = "{}"
# create an instance of Address from a JSON string
address_instance = Address.from_json(json)
# print the JSON string representation of the object
print(Address.to_json())

# convert the object into a dict
address_dict = address_instance.to_dict()
# create an instance of Address from a dict
address_from_dict = Address.from_dict(address_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


