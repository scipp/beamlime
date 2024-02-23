# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from typing import NewType, TypeVar

from beamlime.constructors import ProviderGroup

# Random Events Generation
RandomSeed = NewType("RandomSeed", int)
EventRate = NewType("EventRate", int)  # [events/s]
NumPixels = NewType("NumPixels", int)  # [pixels/detector]
FrameRate = NewType("FrameRate", int)  # [Hz]
NumFrames = NewType("NumFrames", int)  # [dimensionless]
DataFeedingSpeed = NewType("DataFeedingSpeed", float)  # [s/counts]

# Workflow
ChunkSize = NewType("ChunkSize", int)
HistogramBinSize = NewType("HistogramBinSize", int)

P = TypeVar("P")


@dataclass
class TypedParameterContainerMixin:
    @property
    def name_type_map(self) -> dict[str, type]:
        from typing import get_type_hints

        annotations = get_type_hints(self.__class__)
        return annotations

    @property
    def type_name_map(self) -> dict[type, str]:
        return {
            field_type: field_name
            for field_name, field_type in self.name_type_map.items()
        }

    def as_type_dict(self) -> dict[type[P], P]:
        from dataclasses import asdict

        type_name_map = self.type_name_map
        values = asdict(self)
        return {
            field_type: values[field_name]
            for field_type, field_name in type_name_map.items()
        }


@dataclass
class PrototypeParameters(TypedParameterContainerMixin):
    """All configurable parameters for prototypes."""

    num_pixels: NumPixels = NumPixels(10_000)
    event_rate: EventRate = EventRate(10_000)
    histogram_bin_size: HistogramBinSize = HistogramBinSize(50)
    chunk_size: ChunkSize = ChunkSize(28)
    frame_rate: FrameRate = FrameRate(14)
    num_frames: NumFrames = NumFrames(140)
    random_seed: RandomSeed = RandomSeed(123)
    data_feeding_speed: DataFeedingSpeed = DataFeedingSpeed(0.0)


def collect_default_param_providers():
    from functools import partial

    default_param_providers = ProviderGroup()
    for param_type, param_default_value in PrototypeParameters().as_type_dict().items():
        default_param_providers[param_type] = partial(lambda x: x, param_default_value)

    return default_param_providers
