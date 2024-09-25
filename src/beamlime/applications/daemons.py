# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import asyncio
import json
import os
from collections.abc import AsyncGenerator, Generator, Mapping
from dataclasses import dataclass
from typing import NewType

import h5py
import numpy as np

from ..logging import BeamlimeLogger
from ._nexus_helpers import (
    DeserializedMessage,
    NexusPath,
    NexusTemplate,
    RunStartInfo,
    StreamModuleKey,
    StreamModuleValue,
    collect_streaming_modules,
    find_nexus_structure,
    iter_nexus_structure,
)
from ._random_data_providers import (
    EV44,
    DataFeedingSpeed,
    DetectorName,
    EventRate,
    FrameRate,
    NumFrames,
    nxevent_data_ev44_generator,
    random_ev44_generator,
)
from .base import Application, DaemonInterface, MessageProtocol

Path = str | bytes | os.PathLike


@dataclass
class RunStart:
    content: RunStartInfo


@dataclass
class DataPiece:
    key: StreamModuleKey
    deserialized: DeserializedMessage


@dataclass
class DataPieceReceived:
    content: DataPiece


NexusTemplatePath = NewType("NexusTemplatePath", str)
NexusFilePath = NewType("NexusFilePath", str)

EventDataSourcePath = NewType("EventDataSourcePath", str)


def read_nexus_template_file(path: NexusTemplatePath) -> NexusTemplate:
    with open(path) as f:
        return NexusTemplate(json.load(f))


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
    *,
    streaming_modules: dict[StreamModuleKey, StreamModuleValue],
    nexus_structure: Mapping,
    event_rate: EventRate,
    frame_rate: FrameRate,
    event_data_source_path: EventDataSourcePath | None = None,
) -> dict[StreamModuleKey, Generator[dict[str, np.ndarray] | EV44]]:
    detectors = _find_groups_by_nx_class(nexus_structure, nx_class='NXdetector')
    monitors = _find_groups_by_nx_class(nexus_structure, nx_class='NXmonitor')

    ev44_modules = {
        key: value
        for key, value in streaming_modules.items()
        if key.module_type == 'ev44'
    }

    generators: dict[StreamModuleKey, Generator[dict[str, np.ndarray] | EV44]] = {}
    for key, value in ev44_modules.items():
        # TODO: Geometry should be parsed from a file instead of nexus_structure.
        if (det_path := detectors.get(value.path[:-1])) is not None:
            det_group = find_nexus_structure(det_path, ('detector_number',))
            detector_numbers = det_group['config']['values']
        elif value.path[:-1] in monitors:
            detector_numbers = None
        else:
            raise ValueError(f"Detector or monitor group not found for {value.path}")
        if (
            event_data := _try_load_nxevent_data(
                file_path=event_data_source_path, group_path=value.path
            )
        ) is not None:
            generators[key] = nxevent_data_ev44_generator(
                source_name=DetectorName(key.source), **event_data
            )
        else:
            generators[key] = random_ev44_generator(
                source_name=DetectorName(key.source),
                detector_numbers=detector_numbers,
                event_rate=event_rate,
                frame_rate=frame_rate,
            )
    return generators


def _find_groups_by_nx_class(
    nexus_structure: Mapping, nx_class: str
) -> dict[NexusPath, Mapping]:
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
        nexus_template: NexusTemplate,
        nexus_file_path: NexusFilePath,
        num_frames: NumFrames,
        event_rate: EventRate,
        frame_rate: FrameRate,
        event_data_source_path: EventDataSourcePath | None = None,
    ):
        self.logger = logger

        self.nexus_structure = nexus_template
        self.nexus_file_path = nexus_file_path

        self.random_event_generators = fake_event_generators(
            nexus_structure=self.nexus_structure,
            event_data_source_path=event_data_source_path,
            event_rate=event_rate,
            frame_rate=frame_rate,
            streaming_modules=collect_streaming_modules(self.nexus_structure),
        )
        self.data_feeding_speed = speed
        self.num_frames = num_frames

    async def run(self) -> AsyncGenerator[MessageProtocol, None]:
        self.info("Fake data streaming started...")
        # Real listener will wait for the RunStart message to parse the structure
        streaming_modules = collect_streaming_modules(self.nexus_structure)
        self.debug(f"Streaming modules: {streaming_modules}")

        yield RunStart(
            content=RunStartInfo(
                filename=self.nexus_file_path,
                streaming_modules=streaming_modules,
            )
        )

        # Real listener will subscribe to the topics in the ``streaming_modules``
        # and wait for the messages to arrive.
        for i_frame in range(self.num_frames):
            for key, event_generator in self.random_event_generators.items():
                self.info(f"Frame #{i_frame}: sending neutron events for {key}.")
                yield DataPieceReceived(
                    content=DataPiece(key=key, deserialized=next(event_generator))
                )

            self.info(f"Neutron events of frame #{i_frame} were sent.")
            await asyncio.sleep(self.data_feeding_speed)

        yield Application.Stop(content=None)
        self.info("Fake data streaming finished...")

    @classmethod
    def add_argument_group(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group('Fake Listener Configuration')
        group.add_argument(
            "--nexus-template-path",
            help="Path to the nexus template file.",
            type=str,
            required=True,
        )
        group.add_argument(
            "--nexus-file-path",
            help="Path to the nexus file that contains static information.",
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
        with open(args.nexus_template_path) as f:
            nexus_template = json.load(f)
        return cls(
            logger=logger,
            speed=DataFeedingSpeed(args.data_feeding_speed),
            nexus_template=nexus_template,
            nexus_file_path=NexusFilePath(args.nexus_file_path),
            event_data_source_path=EventDataSourcePath(args.event_data_source_path),
            num_frames=NumFrames(args.num_frames),
            event_rate=EventRate(args.event_rate),
            frame_rate=FrameRate(args.frame_rate),
        )
