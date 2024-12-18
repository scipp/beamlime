# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# ruff: noqa: E402

import importlib.metadata

try:
    __version__ = importlib.metadata.version(__package__ or __name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

del importlib

from .core import (
    ConfigManager,
    Service,
    Processor,
    Handler,
    HandlerRegistry,
    StreamProcessor,
    MessageKey,
    MessageSource,
    MessageSink,
    Message,
    compact_messages,
)
from .workflow_protocols import WorkflowResult, LiveWorkflow

__all__ = [
    'compact_messages',
    'ConfigManager',
    'Handler',
    'HandlerRegistry',
    'LiveWorkflow',
    'Message',
    'MessageKey',
    'MessageSink',
    'MessageSource',
    'Processor',
    'Service',
    'StreamProcessor',
    'WorkflowResult',
]
