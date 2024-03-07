# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)

# flake8: noqa
import importlib.metadata

try:
    __version__ = importlib.metadata.version(__package__ or __name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"

del importlib

from .constructors import Factory, Provider, ProviderGroup, SingletonProvider
from .core.protocols import LoggingProtocol
from .core.schedulers import async_retry, retry
from .logging.mixins import LogMixin
from .stateless_workflow import StatelessWorkflow, WorkflowResult
