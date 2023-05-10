# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from queue import Queue
from typing import Iterable, Optional

from ..core.protocols import BeamlimeApplicationProtocol


def validate_queue_connection(
    sender: BeamlimeApplicationProtocol,
    receivers: list[BeamlimeApplicationProtocol],
    raise_error: bool = True,
) -> Optional[bool]:
    for receiver in receivers:
        if receiver.input_channel is not sender.output_channel:
            if raise_error:
                raise RuntimeError(
                    "Application communication pipe broken. "
                    "Please check the application mapping."
                )
            else:
                return False
    return True


def glue(
    sender: BeamlimeApplicationProtocol,
    receivers: Iterable[BeamlimeApplicationProtocol],
) -> None:
    # TODO: Move this to QueueHandler method.
    # Always create and assign output-channel-queue of sender before
    # input-channel-queue of receiver.
    if sender.output_channel is None:
        new_queue = Queue(maxsize=100)
        sender.output_channel = new_queue
    for receiver in receivers:
        if receiver.input_channel is None:
            receiver.input_channel = sender.output_channel

    validate_queue_connection(sender, receivers)
