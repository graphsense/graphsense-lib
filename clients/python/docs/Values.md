# Values

Values model with fiat conversion.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**fiat_values** | [**List[Rate]**](Rate.md) |  |
**value** | **int** |  |

## Example

```python
from graphsense.models.values import Values

# TODO update the JSON string below
json = "{}"
# create an instance of Values from a JSON string
values_instance = Values.from_json(json)
# print the JSON string representation of the object
print(Values.to_json())

# convert the object into a dict
values_dict = values_instance.to_dict()
# create an instance of Values from a dict
values_from_dict = Values.from_dict(values_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
