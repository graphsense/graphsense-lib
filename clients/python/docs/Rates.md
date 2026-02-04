# Rates

Exchange rates model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**rates** | [**List[Rate]**](Rate.md) |  | [optional]
**height** | **int** |  | [optional]

## Example

```python
from graphsense.models.rates import Rates

# TODO update the JSON string below
json = "{}"
# create an instance of Rates from a JSON string
rates_instance = Rates.from_json(json)
# print the JSON string representation of the object
print(Rates.to_json())

# convert the object into a dict
rates_dict = rates_instance.to_dict()
# create an instance of Rates from a dict
rates_from_dict = Rates.from_dict(rates_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
