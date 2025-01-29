# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
# ruff: noqa: E402, F401, I

import importlib.metadata

try:
    __version__ = importlib.metadata.version(__package__ or __name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

del importlib

from .core import (
    CommonHandlerFactory,
    ConfigSubscriber,
    Handler,
    Message,
    MessageKey,
    MessageSink,
    MessageSource,
    Processor,
    Service,
    ServiceBase,
    StreamProcessor,
    compact_messages,
)
from .workflow_protocols import LiveWorkflow, WorkflowResult

__all__ = [
    "CommonHandlerFactory",
    "ConfigSubscriber",
    "Handler",
    "LiveWorkflow",
    "Message",
    "MessageKey",
    "MessageSink",
    "MessageSource",
    "Processor",
    "Service",
    "ServiceBase",
    "StreamProcessor",
    "WorkflowResult",
    "compact_messages",
]
