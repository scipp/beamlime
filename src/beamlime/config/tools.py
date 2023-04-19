# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# ``beamlime.config.tools`` should only contain general helper functions
# that are repeatedly used by various ``beamlime`` package for configuration parsing.
# Therefore it should not have any dependencies on other modules/classes/functions.

from importlib import import_module
from typing import Any, AnyStr, Iterable, List, Union


def nested_data_get(nested_obj: Iterable, *indices: Union[List, str, int]) -> Any:
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

    def _nested_data_get(nested_obj: Iterable, *indices) -> Any:
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


class DictionaryItemOverwrittenWarning(Warning):
    """Overwriting dictionary item might be unexpected."""


class DictionaryItemOverwrittenError(Exception):
    """Overwriting dictionary item is not expected at all."""


def list_to_dict(
    items: List,
    key_field: Union[AnyStr, int] = "name",
    value_field: Union[AnyStr, int] = None,
    allow_overwriting: bool = False,
) -> dict:
    """
    Converts a list to a dictionary based on the ``key_field`` and ``value_field``.
    From the converted dictionary,
    each ``item`` will be accessible by it's own member, which is ``item[key_field]``.

    If ``value_field`` is not defined, it will keep all fields of each item, otherwise
    each item will only keep ``item[value_field]`` in the converted dictionary.

    This helper was needed to prevent unexpected overwriting in the configuration file.
    See https://github.com/scipp/beamlime/discussions/33 for more details.

    You can also give a list of indices as a ``key_field`` but not recommended.

    Examples
    --------
    ```
    >>> items = [{'name': 'lime0', 'price': 1}, {'name': 'lime1', 'price': 2}]
    >>> dict_items = list_to_dict(items, key_field='name')
    >>> dict_items
    {'lime0': {'name': 'lime0', 'price': 1}, 'lime1': {'name': 'lime1', 'price': 2}}

    >>> smaller_dict_items = list_to_dict(items, key_field='name', value_field='price')
    >>> smaller_dict_items
    {'lime0': 1, 'lime1': 2}

    ```

    Raises
    ------
    DictionaryItemOverwrittenWarning
      If ``allow_overwriting`` == True and any values of ``key_field`` are not unique.

    DictionaryItemOverwrittenError
      If ``allow_overwriting`` == False and any values of ``key_field`` are not unique.
    """
    if value_field is None:
        converted_dict = {item[key_field]: item for item in items}
    else:
        converted_dict = {item[key_field]: item[value_field] for item in items}

    if len(converted_dict) != len(items):  # If any items are overwritten.
        message = f"'{key_field}' contains non-unique values. "
        if allow_overwriting:
            raise DictionaryItemOverwrittenWarning(message)
        else:
            hint = "If it is expected you may set the `allow_overwriting` as `True`."
            raise DictionaryItemOverwrittenError(message, hint)

    return converted_dict


def find_home(env_var_key: str = "BEAMLIME_HOME") -> str:
    """
    Returns the home directory of the ``beamlime`` applications,
    where it saves logs and temporary data.
    Setting an environment variable will overwrite the default home directory,
    which is ``~/.beamlime``.

    Example
    -------
    ```
    export BEAMLIME_HOME='another/directory/you/want/to/be/home'
    ```
    """
    import os
    from pathlib import Path

    from beamlime import __name__

    return os.environ.get(env_var_key, Path.home().joinpath("." + __name__))


def import_object(path: str) -> Any:
    """
    Return the object by the path.

    ```
    >>> from beamlime.config.preset_options import HOME_DIR as home_dir_0
    >>> home_dir_1 = import_object("beamlime.config.preset_options.HOME_DIR")
    >>> home_dir_0 is home_dir_1
    True

    ```
    """
    parent_name = ".".join(path.split(".")[:-1])
    obj_name = path.split(".")[-1]

    if len(parent_name) > 0:
        parent_module = import_module(parent_name)
    else:
        from beamlime.config import preset_options

        parent_module = preset_options

    return getattr(parent_module, obj_name)


def wrap_item(
    item_obj: Union[str, list, tuple, int], wrapper: type
) -> Union[str, list, tuple, int]:
    """
    Wrap an item in an iterable to have a desired type to avoid repeated type checking.
    If a non-iterable item(str, int) is given and the desired type is iterable,
    it will create a list that only contains the non-iterable item.

    This helper function was needed to allow 0 or more items in the configuration.
    """
    if isinstance(item_obj, wrapper):
        return item_obj
    elif wrapper in (list, tuple) and isinstance(item_obj, (str, int)):
        return wrapper([item_obj])
    elif (
        wrapper in (list, tuple)
        and isinstance(item_obj, (list, tuple))
        or wrapper in (str, int)
        and isinstance(item_obj, (str, int))
    ):
        return wrapper(item_obj)

    raise ValueError(
        "Item wrapping only possible for: "
        "[tuple->list, list->tuple, int->list, int->tuple, str->list, str->tuple]."
    )
