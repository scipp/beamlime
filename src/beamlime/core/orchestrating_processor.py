# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
# TODO
# 1. Reset mechanism ignores start_time. Either change schema, or use it?
# 2. Make it work with any service, not just data_reduction.
#    - service_name
#    - adapters for accumulators other than StreamProcessor
# 3. Output key naming (needs frontend changes)
# 4. JobId handling, expose to frontend?
# 5. Include start_time and end_time in result messages.
# 6. Actually batch messages from N pulses.
# 7. Remove PeriodAccumulatingHandler, once no longer in use.
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Generic

from ..config.models import ConfigKey, StartTime
from ..config.workflow_spec import WorkflowConfig, WorkflowStatus, WorkflowStatusType
from ..handlers.config_handler import ConfigHandler
from .handler import Accumulator, HandlerFactory, HandlerRegistry, output_stream_name
from .job import JobId, JobManager, JobResult, LegacyJobFactory, WorkflowData
from .message import (
    CONFIG_STREAM_ID,
    Message,
    MessageSink,
    MessageSource,
    StreamId,
    StreamKind,
    Tin,
    Tout,
)


@dataclass(slots=True, kw_only=True)
class MessageBatch:
    start_time: int
    end_time: int
    messages: list[Message[Any]]


class NaiveMessageBatcher:
    def __init__(
        self, batch_length_s: float = 1.0, pulse_length_s: float = 1.0 / 14
    ) -> None:
        # Batch length is currently ignored.
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)
        self._pulse_length_ns = int(pulse_length_s * 1_000_000_000)

    def batch(self, messages: list[Message[Any]]) -> MessageBatch | None:
        if not messages:
            return None
        messages = sorted(messages)
        # start_time is the lower bound of the batch, end_time is the upper bound, both
        # in multiples of the pulse length.
        start_time = (
            messages[0].timestamp // self._pulse_length_ns * self._pulse_length_ns
        )
        end_time = (
            (messages[-1].timestamp + self._pulse_length_ns - 1)
            // self._pulse_length_ns
            * self._pulse_length_ns
        )
        return MessageBatch(start_time=start_time, end_time=end_time, messages=messages)


class MessagePreprocessor(Generic[Tin, Tout]):
    """Message preprocessor that handles batches of messages."""

    def __init__(
        self, factory: HandlerFactory[Tin, Tout], logger: logging.Logger | None = None
    ) -> None:
        self._factory = factory
        self._logger = logger or logging.getLogger(__name__)
        self._accumulators: dict[StreamId, Accumulator[Tin, Tout]] = {}

    def _get_accumulator(self, key: StreamId) -> Accumulator[Tin, Tout] | None:
        """Get an accumulator for the given stream ID."""
        if (accumulator := self._accumulators.get(key)) is not None:
            return accumulator
        if (accumulator := self._factory.make_preprocessor(key)) is not None:
            self._accumulators[key] = accumulator
            return accumulator
        return None

    def _preprocess_stream(
        self, messages: list[Message[Tin]], accumulator: Accumulator[Tin, Tout]
    ) -> Tout:
        """Preprocess messages for a single stream using the given accumulator."""
        for message in messages:
            accumulator.add(message.timestamp, message.value)
        # We assume the accumulator is cleared in `get`.
        return accumulator.get()

    def preprocess_messages(self, batch: MessageBatch) -> WorkflowData:
        """
        Preprocess messages before they are sent to the accumulators.
        """
        messages_by_key = defaultdict[StreamId, list[Message]](list)
        for msg in batch.messages:
            messages_by_key[msg.stream].append(msg)

        data: dict[StreamId, Any] = {}
        for key, messages in messages_by_key.items():
            accumulator = self._get_accumulator(key)
            if accumulator is None:
                self._logger.debug('No preprocessor for key %s, skipping messages', key)
                continue
            try:
                data[key] = self._preprocess_stream(messages, accumulator)
            except Exception:
                self._logger.exception('Error pre-processing messages for key %s', key)
        return WorkflowData(
            start_time=batch.start_time, end_time=batch.end_time, data=data
        )


class OrchestratingProcessor(Generic[Tin, Tout]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        source: MessageSource[Message[Tin]],
        sink: MessageSink[Tout],
        handler_registry: HandlerRegistry[Tin, Tout],
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._source = source
        self._sink = sink
        self._message_preprocessor = MessagePreprocessor(
            handler_registry._factory, self._logger
        )
        self._job_manager = JobManager(
            job_factory=LegacyJobFactory(
                instrument=handler_registry._factory.instrument
            )
        )
        self._job_manager_adapter = JobManagerAdapter(self._job_manager)
        self._message_batcher = NaiveMessageBatcher()

        # NOTE We intend to extract the relevant functionality from ConfigHandler as the
        # full mechanism is no longer needed. For now we keep it, so monitor_data and
        # detector_data services still work. We "extract" the relevant functionality
        # by registering actions. The dict-like interface of the ConfigHandler is unused
        # here.
        config_handler = handler_registry.get(CONFIG_STREAM_ID)
        if config_handler is None or not isinstance(config_handler, ConfigHandler):
            raise ValueError(
                f"Config handler not found in registry for stream {CONFIG_STREAM_ID}"
            )
        self._config_handler = config_handler
        self._config_handler.register_action(
            key='workflow_config',
            action=self._job_manager_adapter.set_workflow_with_config,
        )
        self._config_handler.register_action(
            key='start_time', action=self._job_manager_adapter.reset_job
        )

    def process(self) -> None:
        time.sleep(2.0)
        messages = self._source.get_messages()
        self._logger.debug('Processing %d messages', len(messages))
        config_messages: list[Message[Tin]] = []
        data_messages: list[Message[Tin]] = []

        for msg in messages:
            if msg.stream == CONFIG_STREAM_ID:
                config_messages.append(msg)
            else:
                data_messages.append(msg)

        # Handle config messages, which can trigger workflow (re)creation, resets, etc.
        # Pushes WorkflowConfig into JobManager via JobManagerAdapter.
        result_messages = self._config_handler.handle(config_messages)

        message_batch = self._message_batcher.batch(data_messages)
        if message_batch is None:
            self._logger.debug('No data messages to process')
            return

        # Pre-process message batch
        workflow_data = self._message_preprocessor.preprocess_messages(message_batch)

        # Handle data messages with the workflow manager, accumulating data as needed.
        self._job_manager.push_data(workflow_data)

        # TODO Logic to determine when to compute and publish
        results = self._job_manager.compute_results()
        result_messages.extend([_job_result_to_message(result) for result in results])
        self._sink.publish_messages(result_messages)


def _job_result_to_message(result: JobResult) -> Message:
    """
    Convert a workflow result to a message for publishing.
    """
    if result.name == 'monitor_data':
        service_name = 'monitor_data'
        signal_name = ''
    else:
        service_name = 'data_reduction'
        # We probably want to switch to something like
        #   signal_name=f'{result.name}-{result.job_id}'
        # but for now we keep the legacy signal name for frontend compatibility.
        signal_name = f'reduced/{result.source_name}'
    stream_name = output_stream_name(
        service_name=service_name,
        stream_name=result.source_name,
        signal_name=signal_name,
    )

    return Message(
        timestamp=result.start_time,
        stream=StreamId(kind=StreamKind.BEAMLIME_DATA, name=stream_name),
        value=result.data,
    )


class JobManagerAdapter:
    """
    Adapter to convert calls to JobManager into ConfigHandler actions.

    This has two purposes:

    1. We can keep using ConfigHandler until we have fully refactored everything.
    2. We keep the legacy one-source-one-job behavior, replacing old jobs if a new one
       is started. The long-term goal is to change this to a more flexible mechanism,
       but this, too, would require frontend changes.
    """

    def __init__(self, job_manager: JobManager) -> None:
        self._job_manager = job_manager
        self._jobs: dict[str, JobId] = {}

    def reset_job(
        self, source_name: str | None, value: dict
    ) -> list[tuple[ConfigKey, WorkflowStatus]]:
        if source_name is None:
            for source in self._jobs:
                self.reset_job(source_name=source, value=value)
            return []
        # TODO Can we use the start_time?
        _ = StartTime.model_validate(value)
        self._job_manager.reset_job(job_id=self._jobs[source_name])
        return []

    def set_workflow_with_config(
        self, source_name: str | None, value: dict | None
    ) -> list[tuple[ConfigKey, WorkflowStatus]]:
        if source_name is None:
            raise ValueError("source_name cannot be None for set_workflow_with_config")

        config_key = ConfigKey(source_name=source_name, key="workflow_status")

        config = WorkflowConfig.model_validate(value)
        if config.identifier is None:  # New way to stop/remove a workflow.
            if (job_id := self._jobs.pop(source_name, None)) is not None:
                self._job_manager.stop_job(job_id)
                # TODO Not stopped yet, is returning status here the wrong approach?
                status = WorkflowStatus(
                    source_name=source_name, status=WorkflowStatusType.STOPPED
                )
                return [(config_key, status)]
            return []

        try:
            job_id = self._job_manager.schedule_job(
                source_name=source_name, config=config
            )
            self._jobs[source_name] = job_id
        except Exception as e:
            # TODO This system is a bit flawed: If we have a workflow running already
            # it will keep running, but we need to notify about startup errors. Frontend
            # will not be able to display the correct workflow status. Need to come up
            # with a better way to handle this.
            # NOTE This can be fixed using the new JobManager approach, provided that
            # the frontend can display a per-job status, instead of per-source.
            status = WorkflowStatus(
                source_name=source_name,
                status=WorkflowStatusType.STARTUP_ERROR,
                message=str(e),
            )
            return [(config_key, status)]

        status = WorkflowStatus(
            source_name=source_name,
            status=WorkflowStatusType.RUNNING,
            workflow_id=config.identifier,
        )
        return [(config_key, status)]
