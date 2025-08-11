# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Generic, Protocol

import scipp as sc

from .handler import Accumulator, HandlerRegistry, output_stream_name
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
from ..config.workflow_spec import WorkflowConfig, WorkflowId, WorkflowSpec
from ..handlers.stream_processor_factory import StreamProcessorFactory
from ..handlers.workflow_manager import WorkflowManager as LegacyWorkflowManager


class Processor(Protocol):
    """
    Protocol for a processor that processes messages. Used by :py:class:`Service`.
    """

    def process(self) -> None:
        pass


class StreamProcessor(Generic[Tin, Tout]):
    """
    Processor messages from a source using a handler and send results to a sink.

    The source, handler registry, and sink are injected at construction time.
    """

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
        self._handler_registry = handler_registry

    def process(self) -> None:
        messages = self._source.get_messages()

        # - Get config messages.
        # - start_time messages can trigger reset
        # - other config messages may trigger accumulator recreation or change
        # - Looking at data messages, determine where the horizon is.
        # - Process messages up to the horizon.
        # - post-process and publish
        # - Process remaining messages but do not publish (will happen in next run).

        # Remember that the preprocessors are in a sense stateless, since we can
        # directly flush their result into the accumulators after every message batch,
        # even if we have not reached the horizon yet.

        # How to handle config changes? Try to preserve data we got before?
        # - How do we know if we should change the horizon?
        # - How do we know if the config change does not require a reset?
        #   Interesting case study: Changing TOA range requires a reset currently, but
        #   we might want to support changing it without a reset in the future. The
        #   design needs to allow for this.
        #   Maybe it can be specified on the config command message?
        #   Or maybe this would fall under post-processing?

        # Who adds info on time window? Needs to be done by accumulator?

        # We should support having a start_time in the future, e.g., to schedule a data
        # reduction. And if we do that, also support end_time?
        # Maybe this is a way to avoid the implicit reset mechanism? Every config change
        # should come with a start_time (special value to start now)?

        # Allows user to schedule accumulation, e.g., "accumulate this detector slice
        # for 10 seconds".

        # TODO
        # does setting start time need to go into handler? it should only affect the
        # processor in the new mechanism
        # What about implicit resets from changing config such as TOF range?
        # This feels like a really bad and surprising mechanism. Can we make it
        # explicit by (1) keeping backlog in accumulators, (2) notifying users if the
        # config change requires changing the start time? Consider, e.g., histogram of
        # ROI, where we would need to store events or a 3D histogram in the backend.
        # Or, as a simpler example monitor accumulation has a "hidden" clear mechanism
        # when changing the number of bins.
        # Maybe handlers need to be able to inform the processor about the start time,
        # if the handler was forced to reset its state?
        # If we ever want to be able to re-process messages after a config change
        # (be it from a local buffer or by manipulating the consumer offsets), where
        # would that be handled? Should the handlers/accumulators deal with buffering
        # (which could be more efficient, avoiding full re-processing, but also more
        # complex), or should the processor handle it, simply re-feeding everything into
        # handlers? Maybe handlers could then avoid having to deal with config changes:
        # A handlers is a concrete "job" for a fixed config. If the config changes,
        # the processor can simply create a new handler for the new config and re-feed
        # all messages into it.
        # This sounds like a better design, but it would strictly limit the length of
        # the backlog. In cases where we simply accumulate a histogram before rebinning
        # we could in principle keep an infinite history if done by the handler. Can we
        # combine the benefits of both approaches?
        # Does this fit in between preprocessor and accumulator, currently encapsulated
        # in the handler? But not all preprocessors provide and infinite history, e.g.,
        # grouping events into pixels on keeps current chunk.
        #
        # Could such a mechanism also make it easier to deal with things such as
        # multiple ROIs? Currently we have one fixed one, but maybe we can be more
        # flexible with the approach considered above.
        #
        # Maybe all of the above goes to far. But removing the complexity of
        # specific accumulators within specific handlers "watching" for config changes
        # and resetting themselves would be a good step forward.
        # The processor could then simply re-create the handler for the new config,
        # which would then have to deal with the backlog itself, e.g., by storing
        # messages in a buffer until it is ready to process them.
        #
        # Would this bring us closer to how the workflow manager handles this for data
        # reduction, where a config change (re)creates a stream processor (linked to
        # within a handler)? The mechanism is a bit complex due to this wrapping, but
        # maybe we can make this a first class citizen in the processor? Instead of
        # recreating a processor within a handler, simply re-create the entire handler?
        # Note that the workflow manager deals with the chicken/egg problem of creating
        # a handler before we can create an actual processor. But if config changes can
        # cause handler (re)creation the need for this might go away, simplifying the
        # entire mechanism? See `WorkflowManager.get_accumulator` and how it is used in
        # `ReductionHandlerFactory.make_handler`.
        # ... but does this work? We have monitor streams that are multiplexed into
        # multiple processors.
        messages = self._source.get_messages()
        messages_by_key = defaultdict(list)
        for msg in messages:
            messages_by_key[msg.stream].append(msg)

        results = []
        for key, msgs in messages_by_key.items():
            handler = self._handler_registry.get(key)
            if handler is None:
                self._logger.debug('No handler for key %s, skipping messages', key)
                continue
            try:
                results.extend(handler.handle(msgs))
            except Exception:
                self._logger.exception('Error processing messages for key %s', key)
        self._sink.publish_messages(results)


class Horizon:
    """
    Represents a time horizon for processing messages.

    This should operate based on source pulses, so we have a clear discrete time unit.

    Mechanism:

    - For each source, feed in message timestamps.
    - Determine which messages to process, which to keep for later (or have 2 handlers
      for a sort of double buffering).
    - Tells us when we can produce an update and move to the next horizon.
    - Mechanism to determine when to produce and update can have a backoff, i.e., if
      issues with messages ordering are detected, we can wait for a few pulses before
      producing an update.
    - Consider implementing a precise mechanism to handle setting a start time. Either
      with a local buffer or using Kafka offsets.

    Questions:

    - What about "slow" data sources, succh as monitor readouts published at a lower
      frequency? Do we need to delay starting a time window until we have to first such
      message?

    Assumptions:

    - The pulse time is precise enough for aligning messages from different sources. For
      example, if the monitor-detector distance is large, there is an offset between
      the two, i.e., looking at the pulse time in respective messages will yield events
      that actually originated in pulse N-1 in the same pulse as monitor readings in
      pulse N.

    """


@dataclass(slots=True, kw_only=True)
class MessageBatch:
    start_time: int
    end_time: int
    messages: list[Message[Any]]


@dataclass(slots=True, kw_only=True)
class WorkflowData:
    start_time: int
    end_time: int
    data: dict[StreamId, Any]


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


class Job:
    def __init__(self, processor: StreamProcessor) -> None:
        self._processor = processor

    @staticmethod
    def from_legacy(
        legacy_manager: LegacyWorkflowManager, source_name: str, config: WorkflowConfig
    ) -> Job:
        stream_processor = legacy_manager._processor_factory.create(
            source_name=source_name, config=config
        )
        pass

    @property
    def start_time(self) -> int:
        # TODO Add start_time field to WorkflowConfig
        return -1

    @property
    def end_time(self) -> int:
        # TODO Add end_time field to WorkflowConfig
        return -1

    # TODO Would it be a better interface if the workflow can decide itself when to
    # compute results, e.g., based on a time horizon?
    # Maybe WorkflowSpec should not just have start_time and end_time but also
    # compute_period?
    # Complication: Preprocessed data does not have raw time stamps any more,
    # how can we determine if we reached the period?
    # We still need some sort of source pulse tracking.
    def start(self) -> None:
        # TODO Implement starting logic, e.g., initializing accumulators or handlers.
        pass

    def add(self, data: WorkflowData) -> None:
        # TODO Add aux_sources to WorkflowSpec
        for stream, value in data.data.items():
            if self._key in self._processor._context_keys:
                self._processor.set_context({self._key: value})
            elif self._key in self._processor._dynamic_keys:
                self._processor.accumulate({self._key: value})
            else:
                # Might be unused by this particular workflow
                pass

    def get(self) -> JobResult | None: ...
    def reset(self, start_time: int) -> None: ...


@dataclass
class JobResult:
    start_time: int
    end_time: int
    source_name: str
    name: str
    data: sc.DataArray | sc.DataGroup

    def to_message(self, service_name: str) -> Message:
        """
        Convert the workflow result to a message for publishing.

        Parameters
        ----------
        service_name:
            The name of the service to which the message belongs.
        """
        stream_name = output_stream_name(
            service_name=service_name,
            stream_name=self.source_name,
            signal_name=self.name,
        )
        return Message(
            timestamp=self.start_time,
            stream=StreamId(kind=StreamKind.BEAMLIME_DATA, name=stream_name),
            value=self.data,
        )


class WorkflowRegistry:
    def __init__(self, legacy_manager: LegacyWorkflowManager) -> None:
        self._workflow_specs: dict[WorkflowId, WorkflowSpec] = {}
        self._legacy_manager = legacy_manager

    def create(self, *, source_name: str, config: WorkflowConfig) -> Job:
        # Note that this initializes the job immediately, i.e., we pay startup cost now.
        stream_processor = self._legacy_manager._processor_factory.create(
            source_name=source_name, config=config
        )
        # TODO Take details from StreamProcessorFactory
        workflow_id = config.identifier
        if workflow_id not in self._workflow_specs:
            raise KeyError(f"Unknown workflow ID: {workflow_id}")
        workflow_spec = self._workflow_specs[workflow_id]
        # Note that workflows are created but not started here.
        return Job(spec=workflow_spec, config=config)


JobId = int


class JobManager:
    def __init__(self, workflow_registry: WorkflowRegistry) -> None:
        self.service_name = 'data_reduction'
        self._last_update: int = 0
        self._workflow_registry = workflow_registry
        self._active_jobs: dict[JobId, Job] = {}
        self._scheduled_jobs: dict[JobId, Job] = {}
        self._finishing_jobs: list[JobId] = []
        self._next_job_id: JobId = 0

    @property
    def active_workflows(self) -> list[Job]:
        """Get the list of active workflows."""
        return list(self._active_jobs.values())

    def _schedule_job(self, workflow: Job) -> None:
        """Start a new job with the given workflow."""
        job_id = self._next_job_id
        self._next_job_id += 1
        self._scheduled_jobs[job_id] = workflow

    def _start_job(self, job: JobId) -> None:
        """Start a new job with the given workflow."""
        workflow = self._scheduled_jobs.pop(job, None)
        if workflow is None:
            raise KeyError(f"Job {job} not found in scheduled jobs.")
        workflow.start()
        self._active_jobs[job] = workflow

    def _advance_to_time(self, start_time: int, end_time: int) -> None:
        to_activate = [
            job
            for job, wf in self._scheduled_jobs.items()
            if wf.start_time <= start_time
        ]
        to_finish = [
            job for job, wf in self._active_jobs.items() if wf.end_time <= end_time
        ]
        for job in to_activate:
            self._start_job(job)
        # Do not remove from active workflows yet, we need to compute results.
        self._finishing_jobs.extend(to_finish)

    def handle_config_messages(self, config_messages: list[Message[Tin]]) -> None:
        """
        Handle configuration messages to update the workflow state.
        This may include resetting accumulators or changing their configuration.
        """
        # Config messages contain WorkflowConfig, which WorkflowFactory can use to
        # create a workflow. This may also schedule a workflow stop. In that case we
        # still need to compute a final result?

    def handle_data_messages(self, data: WorkflowData) -> None:
        """
        Handle data messages by passing them to the appropriate accumulators.
        This may include updating the workflow state based on the preprocessed data.
        """
        self._advance_to_time(data.start_time, data.end_time)
        for workflow in self.active_workflows:
            workflow.add(data)

    def compute_results(self) -> list[Message[Tout]]:
        """
        Compute results from the accumulated data and return them as messages.
        This may include processing the accumulated data and preparing it for output.
        """
        results = [workflow.get() for workflow in self.active_workflows]
        results = [result for result in results if result is not None]
        for job in self._finishing_jobs:
            _ = self._active_jobs.pop(job, None)
        self._finishing_jobs.clear()
        return [result.to_message(service_name=self.service_name) for result in results]


class Preprocessor(Generic[Tin, Tout]):
    def __init__(self, accumulator: Accumulator[Tin, Tout]) -> None:
        """
        Initialize the preprocessor with an accumulator class.

        Parameters
        ----------
        accumulator_cls:
            The accumulator class to use for preprocessing messages. Must be default
            constructable.
        """

        self._accumulator = accumulator

    def __call__(self, messages: list[Message[Tin]]) -> Tout:
        """
        Preprocess messages before they are sent to the accumulator.
        """
        for message in messages:
            self._accumulator.add(message.timestamp, message.value)
        # We assume the accumulater is cleared in `get`.
        return self._accumulator.get()


class PreprocessorRegistry(Generic[Tin, Tout]):
    """Preprocessor registry wrapping a legacy handler registry."""

    def __init__(self, hander_registry: HandlerRegistry[Tin, Tout]) -> None:
        self._handlers = hander_registry
        self._preprocessors: dict[StreamId, Preprocessor[Tin, Tout]] = {}

    def get(self, key: StreamId) -> Preprocessor | None:
        """
        Get a preprocessor for the given stream ID.
        """
        if (preprocessor := self._preprocessors.get(key)) is not None:
            return preprocessor
        if (handler := self._handlers.get(key)) is not None:
            preprocessor = Preprocessor(handler._preprocessor)
            self._preprocessors[key] = preprocessor
            return preprocessor
        return None


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
        self._preprocessor_registry = PreprocessorRegistry(handler_registry)
        legacy_manager = handler_registry._factory._workflow_manager
        self._job_manager = JobManager(
            workflow_registry=WorkflowRegistry(legacy_manager=legacy_manager)
        )
        self._message_batcher = NaiveMessageBatcher()

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

        # Handle config messages, which can trigger workflow (re)creation, resets, etc.
        # TODO This might want to return status messages or similar?
        self._job_manager.handle_config_messages(config_messages)

        message_batch = self._message_batcher.batch(data_messages)
        if message_batch is None:
            self._logger.debug('No data messages to process')
            return

        # Pre-process message batch
        workflow_data = self._preprocess_messages(message_batch)

        # Handle data messages with the workflow manager, accumulating data as needed.
        self._job_manager.handle_data_messages(workflow_data)

        # TODO Logic to determine when to compute and publish
        results = self._job_manager.compute_results()
        self._sink.publish_messages(results)

    def _preprocess_messages(self, batch: MessageBatch) -> WorkflowData:
        """
        Preprocess messages before they are sent to the accumulators.
        """
        messages_by_key = defaultdict[StreamId, list[Message]](list)
        for msg in batch.messages:
            messages_by_key[msg.stream].append(msg)

        data: dict[StreamId, Any] = {}
        for key, messages in messages_by_key.items():
            preprocessor = self._preprocessor_registry.get(key)
            if preprocessor is None:
                self._logger.debug('No preprocessor for key %s, skipping messages', key)
                continue
            try:
                data[key] = preprocessor(messages)
            except Exception:
                self._logger.exception('Error pre-processing messages for key %s', key)
        return WorkflowData(
            start_time=batch.start_time, end_time=batch.end_time, data=data
        )
