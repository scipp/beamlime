# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass

KafkaTopic = str


@dataclass(frozen=True, slots=True, kw_only=True)
class InputStreamKey:
    topic: KafkaTopic
    source_name: str


class StreamMapping:
    def __init__(
        self,
        instrument: str,
        detectors: dict[InputStreamKey, str],
        monitors: dict[InputStreamKey, str],
    ) -> None:
        self.instrument = instrument
        self._detectors = detectors
        self._monitors = monitors

    @property
    def detector_topics(self) -> set[KafkaTopic]:
        """Returns the list of detector topics."""
        return {stream.topic for stream in self.detectors.keys()}

    @property
    def monitor_topics(self) -> set[KafkaTopic]:
        """Returns the list of monitor topics."""
        return {stream.topic for stream in self.monitors.keys()}

    @property
    def detectors(self) -> dict[InputStreamKey, str]:
        """Returns the mapping for detector data."""
        return self._detectors

    @property
    def monitors(self) -> dict[InputStreamKey, str]:
        """Returns the mapping for monitor data."""
        return self._monitors
