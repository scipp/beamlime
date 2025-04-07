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
        *,
        instrument: str,
        detectors: dict[InputStreamKey, str],
        monitors: dict[InputStreamKey, str],
        log_topics: set[KafkaTopic] | None = None,
        beamline_config_topic: str,
    ) -> None:
        self.instrument = instrument
        self._detectors = detectors
        self._monitors = monitors
        # Currently we simply reuse the source_name as the stream name
        self._logs = None
        self._log_topics = log_topics or set()
        self._beamline_config_topic = beamline_config_topic

    @property
    def beamline_config_topic(self) -> KafkaTopic:
        """Returns the beamline config topic."""
        return self._beamline_config_topic

    @property
    def detector_topics(self) -> set[KafkaTopic]:
        """Returns the list of detector topics."""
        return {stream.topic for stream in self.detectors.keys()}

    @property
    def monitor_topics(self) -> set[KafkaTopic]:
        """Returns the list of monitor topics."""
        return {stream.topic for stream in self.monitors.keys()}

    @property
    def log_topics(self) -> set[KafkaTopic]:
        """Returns the list of log topics."""
        return self._log_topics

    @property
    def detectors(self) -> dict[InputStreamKey, str]:
        """Returns the mapping for detector data."""
        return self._detectors

    @property
    def monitors(self) -> dict[InputStreamKey, str]:
        """Returns the mapping for monitor data."""
        return self._monitors

    @property
    def logs(self) -> dict[InputStreamKey, str] | None:
        """Returns the mapping for log data."""
        return self._logs
