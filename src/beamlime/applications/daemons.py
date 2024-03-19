# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import asyncio
import os
from dataclasses import dataclass
from typing import AsyncGenerator, Mapping, NewType, Union

from ..logging import BeamlimeLogger
from ._nexus_helpers import NexusContainer
from ._random_data_providers import (
    DataFeedingSpeed,
    DetectorName,
    DetectorNumberCandidates,
    EventRate,
    FrameRate,
    NumFrames,
    random_ev44_generator,
)
from .base import Application, DaemonInterface, MessageProtocol

Path = Union[str, bytes, os.PathLike]


@dataclass
class RunStart:
    content: NexusContainer


@dataclass
class DetectorDataReceived:
    content: Mapping


NexusTemplatePath = NewType("NexusTemplatePath", str)


class FakeListener(DaemonInterface):
    """Event generator based on the nexus template."""

    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
        speed: DataFeedingSpeed,
        nexus_template_path: NexusTemplatePath,
        num_frames: NumFrames,
        event_rate: EventRate,
        frame_rate: FrameRate,
    ):
        self.logger = logger
        self.nexus_container = NexusContainer.from_template_file(nexus_template_path)
        event_rate_per_detector = int(
            event_rate / max(len(self.nexus_container.detectors), 1)
        )
        detector_names = sorted(self.nexus_container.detectors.keys())
        ev44_source_names = sorted(self.nexus_container.modules['ev44'].keys())

        self.random_event_generators = {
            det_name: random_ev44_generator(
                source_name=DetectorName(ev44_source_name),
                detector_numbers=DetectorNumberCandidates(
                    self.nexus_container.detectors[det_name].pixel_ids
                ),
                event_rate=EventRate(event_rate_per_detector),
                frame_rate=frame_rate,
            )
            for det_name, ev44_source_name in zip(detector_names, ev44_source_names)
            # Assuming the order of the detectors and the ev44 modules are the same
            # For real data stream, it doesn't matter
            # since the data is inserted by the source name.
        }
        self.data_feeding_speed = speed
        self.num_frames = num_frames

    async def run(self) -> AsyncGenerator[MessageProtocol, None]:
        self.info("Fake data streaming started...")

        yield RunStart(content=self.nexus_container)

        for i_frame in range(self.num_frames):
            for event_generator in self.random_event_generators.values():
                yield DetectorDataReceived(content=next(event_generator))

            self.info(f"Detector events of frame #{i_frame} were sent.")
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
        return cls(
            logger=logger,
            speed=DataFeedingSpeed(args.data_feeding_speed),
            nexus_template_path=NexusTemplatePath(args.nexus_template_path),
            num_frames=NumFrames(args.num_frames),
            event_rate=EventRate(args.event_rate),
            frame_rate=FrameRate(args.frame_rate),
        )
