# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Iterable, Mapping, Optional, Tuple

import numpy as np


def _dtype(value):
    if hasattr(value, 'dtype'):
        return str(value.dtype)
    if isinstance(value, str):
        return 'string'
    if isinstance(value, int):
        return 'int'
    if isinstance(value, float):
        return 'float'
    return 'unknown'


def _init_dataset(group, field, value):
    dataset = find_nexus_structure(group, (field,))
    if dataset is None:
        dataset = dict(
            module='dataset',
            config=dict(
                name=field,
                dtype=_dtype(value),
                values=value,
            ),
        )
        group['children'].append(dataset)
    return dataset


def _merge_ev44(group, message):
    for field, value in message.items():
        if value is None:  # Could be monitor without pixel_id
            continue
        dataset = _init_dataset(group, field, value)
        if field in ('time_of_flight', 'pixel_id'):
            dataset['config']['values'] = np.concatenate(
                (dataset['config']['values'], value)
            )


def _merge_f144(group, message):
    for field, value in message.items():
        dataset = _init_dataset(group, field, value)
        if field in ('timestamp', 'value'):
            dataset['config']['values'] = np.stack((dataset['config']['values'], value))


def _merge_tdct(group, message):
    for field, value in message.items():
        dataset = _init_dataset(group, field, value)
        if field in ('timestamps',):
            dataset['config']['values'] = np.concatenate(
                (dataset['config']['values'], value)
            )


def _node_name(n):
    '''Defines the name of a nexus tree branch or leaf'''
    config = n.get('config', {})
    return n.get('name', config.get('name'))


def iter_nexus_structure(
    structure: Mapping, root: Optional[Tuple[Optional[str], ...]] = None
) -> Iterable[Tuple[str, Mapping]]:
    '''Visits all branches and leafs in the nexus tree'''
    path = (*root, _node_name(structure)) if root is not None else ()
    yield path, structure
    for child in structure.get('children', []):
        yield from iter_nexus_structure(child, root=path)


def find_nexus_structure(
    structure: Mapping, path: Tuple[Optional[str], ...]
) -> Optional[Mapping]:
    '''Returns the branch or leaf associated with `path`, or None if not found'''
    if len(path) == 0:
        return structure
    head, *tail = path
    for child in structure['children']:
        if head == _node_name(child):
            return find_nexus_structure(child, tail)


def path_to_message_group(structure: Mapping, message: Mapping) -> str:
    '''Finds the group where the message should be merged'''
    kind, content = message
    for path, node in iter_nexus_structure(structure):
        if (
            node.get('module') == kind
            and node.get('config', {}).get('source') == content['source_name']
        ):
            return path


def merge_message_into_store(
    store: Mapping[Tuple[Optional[str], ...], Mapping],
    structure: Mapping,
    message: Tuple[str, Mapping],
):
    '''Merges message into the associated nexus group'''
    path = path_to_message_group(structure, message)
    if path not in store:
        parent = find_nexus_structure(structure, path[:-1])
        store[path] = dict(
            children=[],
            name=_node_name(parent),
        )
        if 'attributes' in parent:
            store[path]['attributes'] = parent['attributes']
        if len(parent['children']) > 1:
            raise ValueError("Multiple modules found in the same data group.")
    kind, content = message
    if kind == 'ev44':
        _merge_ev44(store[path], content)
    elif kind == 'f144':
        _merge_f144(store[path], content)
    elif kind == 'tdct':
        _merge_tdct(store[path], content)
    else:
        raise NotImplementedError


def combine_store_and_structure(
    store: Mapping[Tuple[Optional[str], ...], Mapping], structure: Mapping
):
    '''Creates a new nexus structure, replacing the stream modules
    with the datasets in `store`, while avoiding
    to copy data from `structure` if unessecary'''
    if len(store) == 0:
        return structure
    if (None,) in store:
        return store[(None,)]

    new = structure.copy()
    if 'children' in structure:
        children = []
        for child in structure['children']:
            children.append(
                combine_store_and_structure(
                    {
                        tuple(tail): group
                        for (head, *tail), group in store.items()
                        if head == _node_name(child)
                    },
                    child,
                )
            )
        new['children'] = children
    return new
