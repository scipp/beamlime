# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import asyncio
import os
from dataclasses import dataclass
from typing import AsyncGenerator, NewType, Union

from ._nexus_helpers import NexusContainer
from ._parameters import DataFeedingSpeed, NumFrames
from ._random_data_providers import (
    DetectorName,
    DetectorNumberCandidates,
    random_ev44_generator,
)
from .base import Application, DaemonInterface, MessageProtocol

Path = Union[str, bytes, os.PathLike]


@dataclass
class RunStart:
    content: NexusContainer


@dataclass
class DetectorDataReceived:
    content: dict  # deserialized events


NexusTemplatePath = NewType("NexusTemplatePath", str)


class FakeListener(DaemonInterface):
    """Event generator based on the nexus template."""

    _arg_group_name: str = "Fake Listener Configuration"

    def __init__(
        self,
        *,
        speed: DataFeedingSpeed,
        nexus_template_path: NexusTemplatePath,
        num_frames: NumFrames,
    ):
        self.nexus_container = NexusContainer.from_template_file(nexus_template_path)
        self.random_event_generators = {
            det.detector_name: random_ev44_generator(
                source_name=DetectorName(det.detector_name),
                detector_numbers=DetectorNumberCandidates(det.pixel_ids),
            )
            for det in self.nexus_container.detectors
        }
        self.data_feeding_speed = speed
        self.num_frames = num_frames

    async def run(self) -> AsyncGenerator[MessageProtocol, None]:
        self.info("Fake data streaming started...")

        yield RunStart(content=self.nexus_container)

        for i_frame in range(self.num_frames):
            for gen in self.random_event_generators.values():
                yield DetectorDataReceived(content=next(gen))
            self.info(f"Detector events of frame #{i_frame} was sent.")
            await asyncio.sleep(self.data_feeding_speed)

        yield Application.Stop(content=None)
        self.info("Fake data streaming finished...")

    @classmethod
    def argument_group(cls, base_parser: argparse.ArgumentParser) -> None:
        group = base_parser.add_argument_group(cls._arg_group_name)
        group.add_argument(
            "--data-feeding-speed",
            default=0.1,
            help="Data feeding speed in seconds.",
            type=float,
        )
        group.add_argument(
            "--nexus-template-path",
            default="",
            help="Path to the nexus template file.",
            type=str,
        )
        group.add_argument(
            "--num-frames",
            default=10,
            help="Number of frames to generate.",
            type=int,
        )
