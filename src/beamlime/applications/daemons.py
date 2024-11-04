# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import asyncio
import json
import os
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass
from typing import NewType

import h5py
import numpy as np
import scippnexus as snx
from streaming_data_types.eventdata_ev44 import EventData
from streaming_data_types.logdata_f144 import ExtractedLogData
from streaming_data_types.timestamps_tdct import Timestamps

from ..logging import BeamlimeLogger
from ._nexus_helpers import (
    NexusTemplate,
    RunStartInfo,
    StreamModuleKey,
    _retrieve_groups_by_nx_class,
    collect_streaming_modules,
    collect_streaming_modules_from_nexus_file,
)
from ._random_data_providers import (
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
    deserialized: EventData | Timestamps | ExtractedLogData


@dataclass
class DataPieceReceived:
    content: DataPiece


NexusTemplatePath = NewType("NexusTemplatePath", str)
NexusFilePath = NewType("NexusFilePath", str)


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
        try:
            group = f['/'.join(group_path)]
        except KeyError:
            return
        return {key: group[key][()] for key in group.keys() if key.startswith('event')}


def fake_event_generators(
    *,
    nexus_structure: NexusTemplate | None,
    static_file: NexusFilePath,
    event_rate: EventRate,
    frame_rate: FrameRate,
    event_data_source_path: NexusFilePath | None = None,
) -> dict[StreamModuleKey, Generator[dict[str, np.ndarray] | EventData]]:
    """Create fake event generators based on the nexus structure or static file.

    Parameters
    ----------
    nexus_structure : NexusTemplate | None
        The nexus structure template.
    static_file : NexusFilePath
        The path to the nexus file with static information.
    event_rate : EventRate
        The event rate [Hz].
    frame_rate : FrameRate
        The frame rate [Hz].
    event_data_source_path : NexusFilePath | None
        The path to the nexus file with event data.
        If None, random data will be generated.

    """
    with snx.File(static_file) as f:
        detectors = _retrieve_groups_by_nx_class(f, snx.NXdetector)
        detector_numbers = {
            key: value['detector_number'][()] for key, value in detectors.items()
        }
        monitors = _retrieve_groups_by_nx_class(f, snx.NXmonitor)

    if nexus_structure is not None:
        streaming_modules = collect_streaming_modules(nexus_structure)
    else:
        streaming_modules = collect_streaming_modules_from_nexus_file(static_file)

    ev44_modules = {
        key: value
        for key, value in streaming_modules.items()
        if key.module_type == 'ev44'
    }

    generators: dict[StreamModuleKey, Generator[dict[str, np.ndarray] | EventData]] = {}
    for key, value in ev44_modules.items():
        if (detector_number := detector_numbers.get(value.path[:-1])) is not None:
            detector_number = detector_number.values
        elif value.path[:-1] in monitors:
            detector_number = None
        else:
            raise ValueError(f"Detector or monitor group not found for {value.path}")
        if event_data_source_path is not None:
            event_data = _try_load_nxevent_data(
                file_path=event_data_source_path, group_path=value.path
            )
            if event_data is None:
                raise ValueError(
                    f"NXevent_data not found for {'/'.join(value.path)} "
                    f"in {event_data_source_path}"
                )
            generators[key] = nxevent_data_ev44_generator(
                source_name=DetectorName(key.source), **event_data
            )
        else:
            generators[key] = random_ev44_generator(
                source_name=DetectorName(key.source),
                detector_numbers=detector_number,
                event_rate=event_rate,
                frame_rate=frame_rate,
            )
    return generators


FillDummyData = NewType("FillDummyData", bool)


class FakeListener(DaemonInterface):
    """Event generator based on the nexus template."""

    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
        speed: DataFeedingSpeed,
        nexus_template: NexusTemplate | None = None,
        nexus_file_path: NexusFilePath,
        num_frames: NumFrames,
        event_rate: EventRate,
        frame_rate: FrameRate,
        fill_dummy_data: FillDummyData = False,
    ):
        self.logger = logger

        self.nexus_structure = nexus_template
        self.nexus_file_path = nexus_file_path

        self.random_event_generators = fake_event_generators(
            nexus_structure=self.nexus_structure,
            static_file=nexus_file_path,
            event_rate=event_rate,
            frame_rate=frame_rate,
            event_data_source_path=None if fill_dummy_data else nexus_file_path,
        )
        self.data_feeding_speed = speed
        self.num_frames = num_frames

    async def run(self) -> AsyncGenerator[MessageProtocol, None]:
        self.info("Fake data streaming started...")
        # Real listener will wait for the RunStart message to parse the structure
        if self.nexus_structure is None:
            self.debug(
                "Collecting streaming modules from the nexus file %s",
                self.nexus_file_path,
            )
            streaming_modules = collect_streaming_modules_from_nexus_file(
                self.nexus_file_path
            )
            self.debug("Streaming modules: %s", streaming_modules)
        else:
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
        exhausted_generators = set()
        for i_frame in range(self.num_frames):
            # Handle exhausted generators
            for key in exhausted_generators:
                self.info("Event generator for %s is exhausted. Removing...", key)
                self.random_event_generators.pop(key, None)
            exhausted_generators.clear()

            for key, event_generator in self.random_event_generators.items():
                self.info(f"Frame #{i_frame}: sending neutron events for {key}.")
                try:
                    yield DataPieceReceived(
                        content=DataPiece(key=key, deserialized=next(event_generator))
                    )
                except StopIteration:  # Catch exhausted generators
                    exhausted_generators.add(key)

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
            required=False,
            default=None,
        )
        group.add_argument(
            "--nexus-file-path",
            help="Path to the nexus file with static information and event data.",
            type=str,
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
        group.add_argument(
            "--fill-dummy-data",
            default=False,
            help="Fill the nexus file with dummy data. "
            "If not set, random data will be generated."
            "If set, the path to the nexus file with event data must be provided"
            "using --nexus-file-path option.",
            action="store_true",
        )

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "FakeListener":
        if args.nexus_file_path is None:
            # This option is not set as a required argument in the `add_argument_group`
            # method, because it is only required if fake listener is used.
            raise ValueError(
                "The path to the nexus file is not provided. "
                "Use --nexus-file-path option to set it."
            )

        if args.nexus_template_path is not None:
            with open(args.nexus_template_path) as f:
                nexus_template = json.load(f)
        else:
            nexus_template = None
        return cls(
            logger=logger,
            speed=DataFeedingSpeed(args.data_feeding_speed),
            nexus_template=nexus_template,
            nexus_file_path=NexusFilePath(args.nexus_file_path),
            num_frames=NumFrames(args.num_frames),
            event_rate=EventRate(args.event_rate),
            frame_rate=FrameRate(args.frame_rate),
            fill_dummy_data=args.fill_dummy_data,
        )
