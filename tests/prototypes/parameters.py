# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from typing import NewType

from beamlime.constructors import ProviderGroup

# Random Events Generation
RandomSeed = NewType("RandomSeed", int)
EventRate = NewType("EventRate", int)  # [events/s]
NumPixels = NewType("NumPixels", int)  # [pixels/detector]
FrameRate = NewType("FrameRate", int)  # [Hz]
NumFrames = NewType("NumFrames", int)  # [dimensionless]

# Workflow
ChunkSize = NewType("ChunkSize", int)
HistogramBinSize = NewType("HistogramBinSize", int)


default_param_providers = ProviderGroup()
default_params = {
    RandomSeed: 123,
    FrameRate: 14,
    NumFrames: 140,
    ChunkSize: 28,
    HistogramBinSize: 50,
}

default_param_providers[RandomSeed] = lambda: default_params[RandomSeed]
default_param_providers[FrameRate] = lambda: default_params[FrameRate]
default_param_providers[NumFrames] = lambda: default_params[NumFrames]
default_param_providers[ChunkSize] = lambda: default_params[ChunkSize]
default_param_providers[HistogramBinSize] = lambda: default_params[HistogramBinSize]


@dataclass
class BenchmarkParameters:
    num_pixels: NumPixels
    event_rate: EventRate
    num_frames: NumFrames
    frame_rate: FrameRate


default_param_providers[BenchmarkParameters] = BenchmarkParameters
