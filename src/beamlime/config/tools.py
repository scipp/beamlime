from typing import Iterable, List, Union


def nested_data_get(nested_obj: Iterable, *indices: Union[List, str, int]):
    """
    Helper function to retrieve an item from a nested dictionary or lists.
    If there is no indices give, it will do nothing and
    simply return the ``nested_obj``.

    Example:

    Here is the example nested dictionary you can use this helper on.
    ```
    nested_obj = {
        'apps': [
          {'name': 'app-1'},
          {'name': 'app-2'}
        ]
    }
    ```

    If you want to retrieve ``nested_obj['apps'][1]['name']`` there are 2 ways.

    1. Single ``list`` of indices
    ```
    item = nested_data_get(nested_obj, ['apps', 1, 'name'])
    ```

    2. Single or Multiple ``str`` or `int`` indices
    ```
    item = nested_data_get(nexted_obj, 'apps', 1, 'name')
    ```

    """

    def _nested_data_get(nested_obj: Iterable, *indices):
        """
        Inner function of ``nested_data_get`` to avoid repeated argument wrapping.
        """
        idx = indices[0]
        try:
            child = nested_obj[idx]
        except TypeError:
            raise TypeError(
                f"Index {idx} with type {type(idx)}"
                f"doesn't match the key/index type of {type(nested_obj)}"
            )
        except KeyError:
            raise KeyError(f"{nested_obj} doesn't have the key {idx}")
        except IndexError:
            raise IndexError(f"{idx} is out of the range of {len(nested_obj)-1}")
        if len(indices) == 1:
            return child
        else:
            return _nested_data_get(child, *indices[1:])

    if len(indices) == 0:
        return nested_obj
    elif len(indices) == 1 and isinstance(indices[0], List):
        return _nested_data_get(nested_obj, *indices[0])
    return _nested_data_get(nested_obj, *indices)


def list_to_dict(
    items: list,
    key_field: Union[str, int] = "name",
    value_field: Union[List, str, int] = None,
) -> dict:
    if value_field is None:
        return {item[key_field]: item for item in items}
    elif isinstance(value_field, List):
        {item[key_field]: nested_data_get(item, *value_field) for item in items}
    else:
        return {item[key_field]: item[value_field] for item in items}
