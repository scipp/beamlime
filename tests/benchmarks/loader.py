# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from copy import copy
from dataclasses import dataclass
from typing import NewType, TypeVar, Union

import scipp as sc

from .environments import BenchmarkResultFilePath, BenchmarkRootDir, env_providers
from .runner import BenchmarkReport

loading_providers = copy(env_providers)
ResultPath = BenchmarkResultFilePath


def _reconstruct(contents: Union[dict, list, tuple], target_type: type, strict: bool):
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
    """
    This inheritance is needed to restore the overwritten ``__init__`` method.

    """

    ...


ResultMap = dict[ResultPath, BenchmarkReport]


@loading_providers.provider
def collect_result_paths(root_dir: BenchmarkRootDir) -> list[ResultPath]:
    """Collect all result paths in the benchmark root directory."""
    import os

    return [
        ResultPath(root_dir / file_name)
        for file_name in os.listdir(root_dir)
        if file_name.startswith('result-')
    ]


@loading_providers.provider
def reconstruct_report(path: ResultPath) -> BenchmarkReport:
    """Reconstruct report saved in the ``path``."""
    import json

    return reconstruct_nested_dataclass(json.loads(path.read_text()), ReportTemplate)


@loading_providers.provider
def collect_results(result_paths: list[ResultPath]) -> ResultMap:
    """Collect all results from result_paths."""
    return ResultMap({path: reconstruct_report(path) for path in result_paths})


MergedMeasurements = NewType('MergedMeasurements', sc.Dataset)


@loading_providers.provider
def merge_measurements(results: ResultMap) -> MergedMeasurements:
    """Convert all reports to Dataset and merge."""
    return MergedMeasurements(
        sc.concat([report.asdataset() for report in results.values()], dim='row')
    )
