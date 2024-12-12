# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Any, Generic, Protocol

from .message import Message, MessageKey, Tin, Tout


class Config(Protocol):
    def get(self, key: str, default: Any | None = None) -> Any:
        pass


class ConfigProxy:
    """Proxy for accessing configuration, prefixed with a namespace."""

    def __init__(self, config: Config, *, namespace: str):
        self._config = config
        self._namespace = namespace

    def get(self, key: str, default: Any | None = None) -> Any:
        """
        Look for the key with the namespace prefix, and fall back to the key without
        the prefix if not found. If neither is found, return the default.
        """
        return self._config.get(
            f'{self._namespace}.{key}', self._config.get(key, default)
        )


class Handler(Generic[Tin, Tout]):
    def __init__(self, *, logger: logging.Logger | None, config: Config):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config

    # TODO It is not clear how to handle output topic naming. Should the handler
    # take care of this explicitly? Can there be automatic prefixing as with
    # the config?
    def handle(self, message: Message[Tin]) -> list[Message[Tout]]:
        raise NotImplementedError


class HandlerRegistry(Generic[Tin, Tout]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config: Config,
        handler_cls: type[Handler[Tin, Tout]],
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._handler_cls = handler_cls
        self._handlers: dict[MessageKey, Handler] = {}

    def get(self, key: str) -> Handler:
        if key not in self._handlers:
            self._handlers[key] = self._handler_cls(
                logger=self._logger, config=ConfigProxy(self._config, namespace=key)
            )
        return self._handlers[key]
