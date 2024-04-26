# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from typing import AsyncGenerator, Mapping, NewType, Union

from ..logging import BeamlimeLogger
from ._nexus_helpers import find_nexus_structure, iter_nexus_structure
from ._random_data_providers import (
    DataFeedingSpeed,
    EventRate,
    FrameRate,
    NumFrames,
    random_ev44_generator,
)
from .base import Application, DaemonInterface, MessageProtocol

Path = Union[str, bytes, os.PathLike]


@dataclass
class RunStart:
    content: Mapping


@dataclass
class DetectorDataReceived:
    content: Mapping


@dataclass
class LogDataReceived:
    content: Mapping


@dataclass
class ChopperDataReceived:
    content: Mapping


NexusTemplatePath = NewType("NexusTemplatePath", str)
NexusTemplate = NewType("NexusTemplate", Mapping)
'''A template describing the nexus file structure for the instrument'''


def read_nexus_template_file(path: NexusTemplatePath) -> NexusTemplate:
    with open(path) as f:
        return NexusTemplate(json.load(f))


def fake_event_generators(
    nexus_structure: Mapping,
    event_rate: EventRate,
    frame_rate: FrameRate,
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
            detector_numbers = (
                find_nexus_structure(det, ('detector_number',))['config']['values'],
            )
        elif path in monitors:
            detector_numbers = None
        else:
            raise ValueError(f"Detector or monitor group not found for {path}")
        # Not using ev44_source_name as key, for now at least: We are not using it
        # currently, but have json files with duplicate source names.
        key = '/'.join(path)
        generators[key] = random_ev44_generator(
            source_name=ev44_source_name,
            detector_numbers=detector_numbers,
            event_rate=event_rate,
            frame_rate=frame_rate,
        )
    return generators


def _find_groups_by_nx_class(nexus_structure, nx_class: str) -> dict[str, Mapping]:
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
        num_frames: NumFrames,
        event_rate: EventRate,
        frame_rate: FrameRate,
    ):
        self.logger = logger

        self.nexus_structure = nexus_template

        self.random_event_generators = fake_event_generators(
            self.nexus_structure,
            event_rate,
            frame_rate,
        )
        self.data_feeding_speed = speed
        self.num_frames = num_frames

    async def run(self) -> AsyncGenerator[MessageProtocol, None]:
        self.info("Fake data streaming started...")

        yield RunStart(content=self.nexus_structure)

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
        with open(args.nexus_template_path) as f:
            nexus_template = json.load(f)
        return cls(
            logger=logger,
            speed=DataFeedingSpeed(args.data_feeding_speed),
            nexus_template=nexus_template,
            num_frames=NumFrames(args.num_frames),
            event_rate=EventRate(args.event_rate),
            frame_rate=FrameRate(args.frame_rate),
        )
