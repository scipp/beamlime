# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)


def source_name(device: str, signal: str) -> str:
    """
    Return the source name for a given device and signal.

    This is used to construct the source name from the device name and signal name
    The source_name is used in various Kafka messages.

    ':' is used as the separator in the ECDC naming convention at ESS.
    """
    return f'{device}:{signal}'
