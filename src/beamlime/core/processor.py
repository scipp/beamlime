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
        self._logger.debug('Processing %d messages', len(messages))
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
