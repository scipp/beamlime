# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import json
import logging

from ..core.handler import Config, Handler
from ..core.message import Message, MessageKey
from ..kafka.helpers import beamlime_command_topic


class ConfigHandler(Handler[bytes, None]):
    """
    Handler for configuration messages.

    This handler processes configuration messages and updates the configuration
    dictionary accordingly. It is used by StreamProcessor to handle configuration
    updates received from a Kafka topic.

    Parameters
    ----------
    logger
        Logger to use
    config
        Configuration object to update
    """

    @staticmethod
    def message_key(instrument: str) -> MessageKey:
        return MessageKey(topic=beamlime_command_topic(instrument), source_name='')

    def __init__(self, *, logger: logging.Logger | None = None, config: Config):
        super().__init__(logger=logger, config=config)
        self._store = config

    def get(self, key: str, default=None):
        return self._store.get(key, default)

    def handle(self, messages: list[Message[bytes]]) -> list[Message[None]]:
        """
        Process configuration messages and update the configuration.

        Parameters
        ----------
        messages:
            List of messages containing configuration updates

        Returns
        -------
        :
            Empty list as this handler doesn't produce output messages
        """
        for message in messages:
            try:
                key = message.value['key']
                value = json.loads(message.value['value'].decode('utf-8'))
                self._logger.info(
                    'Updating config: %s = %s at %s', key, value, message.timestamp
                )
                self._store[key] = value
            except Exception as e:  # noqa: PERF203
                self._logger.error('Error processing config message: %s', e)
        return []
