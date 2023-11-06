# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from copy import copy
from dataclasses import dataclass
from typing import TypeVar

from .environments import BenchmarkResultFilePath, BenchmarkRootDir, env_providers
from .runner import BenchmarkReport

loading_providers = copy(env_providers)
ResultPath = BenchmarkResultFilePath


def _reconstruct(contents: dict | list | tuple, target_type: type, strict: bool):
    """Reconstruct"""
    from dataclasses import is_dataclass

    if is_dataclass(target_type) and isinstance(contents, dict):
        return reconstruct_nested_dataclass(contents, target_type, strict)
    elif isinstance(target_type, type) and issubclass(target_type, tuple):
        # Reconstruct ``NamedTuple``.
        # Could not use ``NamedTuple`` since it is not a class.
        return target_type(*contents)
    else:
        return contents


D = TypeVar("D")


def _retrieve_underlying_type(attr_type: type) -> type:
    from typing import get_origin

    from beamlime.constructors.inspectors import extract_underlying_type

    true_type = extract_underlying_type(attr_type)
    return get_origin(true_type) or true_type


def _validate_reconstruction(attr: D, attr_type: type[D]) -> bool:
    return isinstance(attr, _retrieve_underlying_type(attr_type))


def reconstruct_nested_dataclass(
    contents: dict, root_type: type[D], strict: bool = True
) -> D:
    """Reconstructs dataclass object from a dictionary.

    This helper works only if the signiture of ``__init__`` method is not overwritten
    and all dataclass attributes are directly accessible from the object.

    For example, it will not reconstruct a dataclass if it is in a list or dictionary.
    """
    from dataclasses import is_dataclass
    from typing import get_type_hints

    if not is_dataclass(root_type):
        raise ValueError(f"Expected a dataclass type. Got {root_type =}")

    reconstructed = root_type(**contents)
    for attr_name, attr_type in get_type_hints(root_type).items():
        attr = getattr(reconstructed, attr_name)
        reconstructed_attr = _reconstruct(attr, attr_type, strict)
        if strict and not _validate_reconstruction(reconstructed_attr, attr_type):
            raise ValueError(
                "Failed to reconstruct attribute " f"{attr_name} to a right type."
            )

        setattr(reconstructed, attr_name, reconstructed_attr)

    return reconstructed


@dataclass
class ReportTemplate(BenchmarkReport):
    ...


@loading_providers.provider
class Result:
    def __init__(self, file_path: ResultPath) -> None:
        import json

        self.file_path = file_path
        self.report = reconstruct_nested_dataclass(
            json.loads(file_path.read_text()), ReportTemplate
        )


@loading_providers.provider
def collect_result_paths(root_dir: BenchmarkRootDir) -> list[ResultPath]:
    import os

    return [
        ResultPath(root_dir / file_name)
        for file_name in os.listdir(root_dir)
        if file_name.startswith('result-')
    ]


@loading_providers.provider
def collect_results(result_paths: list[ResultPath]) -> list[Result]:
    return [Result(result_path) for result_path in result_paths]
