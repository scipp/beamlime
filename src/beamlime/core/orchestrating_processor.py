# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Generic, Protocol

from .handler import HandlerRegistry
from .message import Message, MessageSink, MessageSource, Tin, Tout


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


class Accumulator(Protocol):  # Workflow?
    def add(self, data: dict[str, sc.DataArray]) -> None: ...
    def get(self, end_time: int): ...
    def reset(self, start_time: int) -> None: ...


class Processor:
    def process(self) -> None:
        config_messages = self.get_config_messages()
        # Encapsulates knowledge about concrete accumulators and corresponding config
        # messages. Same as WorkflowConfig mechanism, using a registry?
        self.configure_accumulators(config_messages)
        data_messages = self.get_data_messages()

        # Does this need to consider any config messages aside from a reset?
        # Do NOT change horizons one resets or config changes! We are moving away from
        # "interactive data exploration". We can have multiple independent workflows
        # running, changing one workflow should not affect others.... and we don't want
        # to track a different horizon for each, I think?
        self.update_horizon(data_messages)
        before, after = self.horizon.split(data_messages)
        if self.horizon.is_ready():
            # Note that `before` may be empty, e.g., if we could not determine in the
            # previous call that we are ready to process.
            data = self._preprocess(before)
            self._accumulate(data)
            self._publish_results()
        data = self._preprocess(after)
        self._accumulate(data)

    def _accumulate(self, data) -> None:
        for accum in self.get_active_accumulators():
            accum.add(data)

    def _publish_results(self, end_time: int) -> None:
        results = [
            accum.get(end_time=end_time)
            for accum in self.get_active_accumulators(end_time=end_time)
        ]
        # publish
        self.horizon.step()

    def reset(self) -> None:
        # ???
        self.reset_accumulators()
        self.reset_horizon()


for batch in self.update_horizon(data_messages):
    data = self._preprocess(batch.messages)
    self._accumulate(data)
    if batch.ready:
        self._publish_results()
