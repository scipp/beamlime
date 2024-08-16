# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import asyncio
import json
import os
from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from typing import NewType, TypedDict

import h5py
import numpy as np

from ..logging import BeamlimeLogger
from ._nexus_helpers import (
    Nexus,
    NexusGroup,
    NexusPath,
    find_nexus_structure,
    iter_nexus_structure,
)
from ._random_data_providers import (
    DataFeedingSpeed,
    EventRate,
    FrameRate,
    NumFrames,
    nxevent_data_ev44_generator,
    random_ev44_generator,
)
from .base import Application, DaemonInterface, MessageProtocol

Path = str | bytes | os.PathLike
NexusSkeletonPath = NewType("NexusSkeletonPath", str)


class RunStartInfo(TypedDict):
    structure: NexusGroup
    filename: Path


@dataclass
class RunStart:
    content: RunStartInfo


@dataclass
class DetectorDataReceived:
    content: Mapping


@dataclass
class LogDataReceived:
    content: Mapping


@dataclass
class ChopperDataReceived:
    content: Mapping


NexusStructurePath = NewType("NexusStructurePath", str)

EventDataSourcePath = NewType("EventDataSourcePath", str)


def read_nexus_template_file(path: NexusStructurePath) -> NexusGroup:
    with open(path) as f:
        return NexusGroup(json.load(f))


def _try_load_nxevent_data(
    file_path: str | None, group_path: tuple[str, ...]
) -> dict[str, np.ndarray] | None:
    """
    Try to load NXevent_data for a given group from a file.

    If found, this will be used instead of random data generation.
    """
    if file_path is None:
        return
    with h5py.File(file_path, 'r') as f:
        group_path = (*group_path, group_path[-1] + '_events')
        try:
            group = f['/'.join(group_path)]
        except KeyError:
            return
        return {key: group[key][()] for key in group.keys() if key.startswith('event')}


def fake_event_generators(
    nexus_structure: NexusGroup,
    event_rate: EventRate,
    frame_rate: FrameRate,
    event_data_source_path: EventDataSourcePath | None = None,
):
    detectors = _find_groups_by_nx_class(nexus_structure, nx_class='NXdetector')
    monitors = _find_groups_by_nx_class(nexus_structure, nx_class='NXmonitor')

    ev44_source_names = {
        # [:-2] trims nested NXevent_data and 'None' (from stream?)
        path[:-2]: node['config']['source']
        for path, node in iter_nexus_structure(nexus_structure)
        if node.get('module') == 'ev44'
    }

    generators = {}
    for path, ev44_source_name in ev44_source_names.items():
        if (det := detectors.get(path)) is not None:
            detector_numbers = find_nexus_structure(det, ('detector_number',))[
                'config'
            ]['values']
        elif path in monitors:
            detector_numbers = None
        else:
            raise ValueError(f"Detector or monitor group not found for {path}")
        # Not using ev44_source_name as key, for now at least: We are not using it
        # currently, but have json files with duplicate source names.
        key = '/'.join(path)
        if (
            event_data := _try_load_nxevent_data(
                file_path=event_data_source_path, group_path=path
            )
        ) is not None:
            generators[key] = nxevent_data_ev44_generator(
                source_name=ev44_source_name, **event_data
            )
        else:
            generators[key] = random_ev44_generator(
                source_name=ev44_source_name,
                detector_numbers=detector_numbers,
                event_rate=event_rate,
                frame_rate=frame_rate,
            )
    return generators


def _find_groups_by_nx_class(
    nexus_structure: NexusGroup, nx_class: str
) -> dict[NexusPath, Nexus]:
    return {
        path: node
        for path, node in iter_nexus_structure(nexus_structure)
        if any(
            attr.get('name') == 'NX_class' and attr.get('values') == nx_class
            for attr in node.get('attributes', ())
        )
    }


class FakeListener(DaemonInterface):
    """Event generator based on the nexus template."""

    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
        speed: DataFeedingSpeed,
        nexus_structure: NexusGroup,
        nexus_template_file: NexusSkeletonPath,
        num_frames: NumFrames,
        event_rate: EventRate,
        frame_rate: FrameRate,
        event_data_source_path: EventDataSourcePath | None = None,
    ):
        self.logger = logger

        self.nexus_structure = nexus_structure
        self.filename = nexus_template_file

        self.random_event_generators = fake_event_generators(
            nexus_structure=self.nexus_structure,
            event_data_source_path=event_data_source_path,
            event_rate=event_rate,
            frame_rate=frame_rate,
        )
        self.data_feeding_speed = speed
        self.num_frames = num_frames

    async def run(self) -> AsyncGenerator[MessageProtocol, None]:
        self.info("Fake data streaming started...")

        yield RunStart(
            content={"structure": self.nexus_structure, "filename": self.filename}
        )

        for i_frame in range(self.num_frames):
            for name, event_generator in self.random_event_generators.items():
                self.info(f"Frame #{i_frame}: sending neutron events for {name}.")
                yield DetectorDataReceived(content=next(event_generator))

            self.info(f"Neutron events of frame #{i_frame} were sent.")
            await asyncio.sleep(self.data_feeding_speed)

        yield Application.Stop(content=None)
        self.info("Fake data streaming finished...")

    @classmethod
    def add_argument_group(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group('Fake Listener Configuration')
        group.add_argument(
            "--nexus-structure-path",
            help="Path to the nexus template file.",
            type=str,
            required=True,
        )
        group.add_argument(
            "--nexus-template-path",
            help="Path to the nexus template file.",
            type=str,
            required=True,
        )
        group.add_argument(
            "--event-data-source-path",
            help="Path to the event data source file.",
            type=str,
            default=None,
        )
        group.add_argument(
            "--data-feeding-speed",
            default=1 / 14,
            help="Data feeding speed [s].",
            type=float,
        )
        group.add_argument(
            "--num-frames",
            default=3,
            help="Number of frames to generate.",
            type=int,
        )
        group.add_argument(
            "--event-rate",
            default=10_000,
            help="Event rate [Hz]. It will be distributed among the detectors.",
            type=int,
        )
        group.add_argument(
            "--frame-rate",
            default=14,
            help="Frame rate [Hz].",
            type=int,
        )

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "FakeListener":
        with open(args.nexus_structure_path) as f:
            nexus_structure = json.load(f)
        return cls(
            logger=logger,
            speed=DataFeedingSpeed(args.data_feeding_speed),
            nexus_structure=nexus_structure,
            nexus_template_file=args.nexus_template_path,
            event_data_source_path=EventDataSourcePath(args.event_data_source_path),
            num_frames=NumFrames(args.num_frames),
            event_rate=EventRate(args.event_rate),
            frame_rate=FrameRate(args.frame_rate),
        )
