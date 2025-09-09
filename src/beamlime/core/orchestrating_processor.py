# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Generic

from ..config.models import ConfigKey, StartTime
from ..config.workflow_spec import WorkflowConfig, WorkflowStatus, WorkflowStatusType
from ..handlers.config_handler import ConfigProcessor
from .handler import Accumulator, HandlerFactory, HandlerRegistry, output_stream_name
from .job import (
    DifferentInstrument,
    JobFactory,
    JobId,
    JobManager,
    JobResult,
    WorkflowData,
)
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
from .message_batcher import MessageBatch, MessageBatcher, SimpleMessageBatcher


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
        message_batcher: MessageBatcher | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._source = source
        self._sink = sink
        self._message_preprocessor = MessagePreprocessor(
            factory=handler_registry._factory, logger=self._logger
        )
        self._job_manager = JobManager(
            job_factory=JobFactory(instrument=handler_registry._factory.instrument)
        )
        self._job_manager_adapter = JobManagerAdapter(
            job_manager=self._job_manager, logger=self._logger
        )
        self._message_batcher = message_batcher or SimpleMessageBatcher()
        self._config_processor = ConfigProcessor(
            job_manager_adapter=self._job_manager_adapter, logger=self._logger
        )

    def process(self) -> None:
        messages = self._source.get_messages()
        self._logger.debug('Processing %d messages', len(messages))
        config_messages: list[Message[Tin]] = []
        data_messages: list[Message[Tin]] = []

        for msg in messages:
            if msg.stream == CONFIG_STREAM_ID:
                config_messages.append(msg)
            else:
                data_messages.append(msg)

        # Handle config messages
        result_messages = self._config_processor.process_messages(config_messages)

        message_batch = self._message_batcher.batch(data_messages)
        if message_batch is None:
            self._logger.debug('No data messages to process')
            self._sink.publish_messages(result_messages)
            if not config_messages:
                # Avoid busy-waiting if there is no data and no config messages.
                # If there are config messages, we avoid sleeping, since config messages
                # may trigger costly workflow creation.
                time.sleep(0.1)
            return
        self._logger.debug(
            'Processing batch with %d data messages', len(message_batch.messages)
        )

        # Pre-process message batch
        workflow_data = self._message_preprocessor.preprocess_messages(message_batch)

        # Handle data messages with the workflow manager, accumulating data as needed.
        job_statuses = self._job_manager.push_data(workflow_data)

        # Log any errors from data processing
        for status in job_statuses:
            if status.has_error:
                self._logger.error(self._job_manager.format_job_error(status))

        # We used to compute results only after 1-N accumulation calls, reasoning that
        # processing data (partially) immediately (instead of waiting for more data)
        # would increase the latency. A closer look, the contrary is true, based on
        # a simple model with a constant plus linear (per event) time for preprocessing
        # (including accumulation).
        results = self._job_manager.compute_results()

        # Filter valid results and log errors
        valid_results = []
        for result in results:
            if result.error_message is not None:
                self._logger.error(
                    'Job %d (%s/%s) failed: %s',
                    result.job_id,
                    result.source_name,
                    result.name,
                    result.error_message,
                )
            else:
                valid_results.append(result)

        result_messages.extend(
            [_job_result_to_message(result) for result in valid_results]
        )
        self._sink.publish_messages(result_messages)


def _job_result_to_message(result: JobResult) -> Message:
    """Convert a workflow result to a message for publishing."""

    # We probably want to switch to something like
    #   signal_name=f'{result.name}-{result.job_id}'
    # but for now we keep the legacy signal name for frontend compatibility.
    service_name = result.namespace
    if service_name == 'monitor_data':
        signal_name = ''
    elif service_name == 'detector_data':
        signal_name = result.name
    else:
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

    def __init__(self, *, job_manager: JobManager, logger: logging.Logger) -> None:
        self._logger = logger
        self._job_manager = job_manager
        self._jobs: dict[str, JobId] = {}

    def reset_job(
        self, source_name: str | None, value: dict
    ) -> list[tuple[ConfigKey, WorkflowStatus]]:
        if source_name is None:
            for source in self._jobs:
                self.reset_job(source_name=source, value=value)
            return []
        # TODO Can we use the start_time or should we change the schema?
        _ = StartTime.model_validate(value)
        self._job_manager.reset_job(job_id=self._jobs[source_name])
        return []

    def set_workflow_with_config(
        self, source_name: str | None, value: dict | None
    ) -> list[tuple[ConfigKey, WorkflowStatus]]:
        if source_name is None:
            raise ValueError("source_name cannot be None for set_workflow_with_config")

        config_key = ConfigKey(
            service_name="job_server", source_name=source_name, key="workflow_status"
        )

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
            if source_name in self._jobs:
                # If we have a job for this source, we stop it first.
                self._job_manager.stop_job(self._jobs[source_name])
            self._jobs[source_name] = job_id
        except DifferentInstrument:
            # We have multiple backend services that handle jobs, e.g., data_reduction
            # and monitor_data. The frontend simply sends a WorkflowConfig message and
            # does not make assumptions which service will handle it. The workflows
            # for each backend are part of a different instrument, e.g., 'dream' for
            # data_reduction and 'dream_beam_monitors' for monitor_data, which is
            # included in the identifier. This should thus work safely, but the question
            # is whether it should be filtered out earlier.
            self._logger.debug(
                "Workflow %s not found, assuming it is handled by another worker",
                config.identifier,
            )
            return []
        except Exception as e:
            # TODO This system is a bit flawed: If we have a workflow running already
            # it will keep running, but we need to notify about startup errors. Frontend
            # will not be able to display the correct workflow status. Need to come up
            # with a better way to handle this.
            # NOTE This can be fixed using the new JobManager approach, provided that
            # the frontend can display a per-job status, instead of per-source.
            # But maybe the key insight is that status-reporting should be decoupled
            # from reply messages. Maybe status should just be emitted periodically for
            # all jobs?
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
