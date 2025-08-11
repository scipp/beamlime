# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Generic, Protocol

from .handler import Accumulator, HandlerRegistry
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

Preprocessed = Any


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


class Workflow:
    def __init__(self, spec: WorkflowSpec, config: WorkflowConfig) -> None:
        """
        Initialize the workflow with a specification and configuration.

        Parameters
        ----------
        spec:
            The workflow specification defining the workflow's parameters and behavior.
        config:
            The configuration for the workflow, including parameter values.
        """
        self._spec = spec
        self._config = config

    @property
    def spec(self) -> WorkflowSpec:
        """
        Get the workflow specification.
        """
        return self._spec

    # TODO Would it be a better interface if the workflow can decide itself when to
    # compute results, e.g., based on a time horizon?
    # Maybe WorkflowSpec should not just have start_time and end_time but also
    # compute_period?
    # Complication: Preprocessed data does not have raw time stamps any more,
    # how can we determine if we reached the period?
    # We still need some sort of source pulse tracking.
    def add(self, data: dict[StreamId, Preprocessed]) -> None: ...
    def get(self, end_time: int): ...
    def reset(self, start_time: int) -> None: ...


class WorkflowRegistry:
    def __init__(self) -> None:
        self._workflow_specs: dict[WorkflowId, WorkflowSpec] = {}

    def create(self, *, source_name: str, config: WorkflowConfig) -> Workflow:
        # TODO Take details from StreamProcessorFactory
        workflow_id = config.identifier
        if workflow_id not in self._workflow_specs:
            raise KeyError(f"Unknown workflow ID: {workflow_id}")
        workflow_spec = self._workflow_specs[workflow_id]
        return Workflow(spec=workflow_spec, config=config)


class WorkflowManager:
    def __init__(self, workflow_registry: WorkflowRegistry) -> None:
        self._last_update: int = 0
        self._workflow_registry = workflow_registry

    def advance_to_time(self, new_time: int) -> None:
        # Latest or incremented to next end_time??
        # Who determines when to compute/publish?
        pass

    def get_active_workflows(self) -> list[Workflow]:
        """
        Get a list of active workflows.
        This also starts scheduled workflows, if their start_time is reached.
        """
        # Should this really start workflows? We don't want to necessarily tie this
        # to data handling, but rather to the time horizon.
        return []

    def handle_config_messages(self, config_messages: list[Message[Tin]]) -> None:
        """
        Handle configuration messages to update the workflow state.
        This may include resetting accumulators or changing their configuration.
        """
        # Config messages contain WorkflowConfig, which WorkflowFactory can use to
        # create a workflow. This may also schedule a workflow stop. In that case we
        # still need to compute a final result?

    def handle_data_messages(self, data: dict[StreamId, Preprocessed]) -> None:
        """
        Handle data messages by passing them to the appropriate accumulators.
        This may include updating the workflow state based on the preprocessed data.
        """
        # Multiplex data into the active workflows.
        for workflow in self.get_active_workflows():
            workflow.add(data)

    def compute_results(self, end_time: int) -> list[Message[Tout]]:
        """
        Compute results from the accumulated data and return them as messages.
        This may include processing the accumulated data and preparing it for output.
        """
        self._last_update = end_time
        results = [
            workflow.get(end_time=end_time) for workflow in self.get_active_workflows()
        ]
        # Where do we get this info?
        # WorkflowConfig messages have:
        # - source_name
        # - name (via WorkflowSpec) or identifier -> use as signal name?
        #   previously this was the accumulator name, now we have one workflow per
        #   accumulator
        return [
            Message(
                timestamp=timestamp,
                stream=StreamId(
                    kind=StreamKind.BEAMLIME_DATA,
                    name=output_stream_name(
                        service_name=self._service_name,
                        stream_name=key.name,
                        signal_name=name,
                    ),
                ),
                value=accumulator.get(),
            )
            for name, accumulator in self._accumulators.items()
        ]


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


class OrchestratingProcessor:
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
        self._workflow_manager = WorkflowManager()

    def process(self) -> None:
        messages = self._source.get_messages()
        self._logger.debug('Processing %d messages', len(messages))
        messages_by_key = defaultdict(list)
        for msg in messages:
            messages_by_key[msg.stream].append(msg)

        # Handle config messages, which can trigger workflow (re)creation, resets, etc.
        config_messages = messages_by_key.pop(CONFIG_STREAM_ID, [])
        # TODO This might want to return status messages or similar?
        self._workflow_manager.handle_config_messages(config_messages)

        # Pre-process data messages
        preprocessed = self._preprocess_messages(messages_by_key)

        # Handle data messages with the workflow manager, accumulating data as needed.
        self._workflow_manager.handle_data_messages(preprocessed)

        # TODO Logic to determine when to compute and publish
        results = self._workflow_manager.compute_results(end_time=0)
        self._sink.publish_messages(results)

    def _preprocess_messages(
        self, messages_by_key: dict[StreamId, list[Message[Tin]]]
    ) -> dict[StreamId, Preprocessed]:
        """
        Preprocess messages before they are sent to the accumulators.
        """
        preprocessed: dict[StreamId, Preprocessed] = {}
        for key, msgs in messages_by_key.items():
            preprocessor = self._preprocessor_registry.get(key)
            if preprocessor is None:
                self._logger.debug('No preprocessor for key %s, skipping messages', key)
                continue
            try:
                preprocessed[key] = preprocessor(msgs)
            except Exception:
                self._logger.exception('Error pre-processing messages for key %s', key)
        return preprocessed
