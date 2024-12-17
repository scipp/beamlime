# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)

from beamlime import Message, compact_messages


def test_comparison_compares_timestamps() -> None:
    msg1 = Message(timestamp=1, key="key1", value="value1")
    msg2 = Message(timestamp=2, key="key1", value="value2")
    msg3 = Message(timestamp=3, key="key0", value="value3")
    msg4 = Message(timestamp=3, key="key1", value="value3")
    assert msg1 < msg2
    assert msg2 < msg3
    assert msg1 < msg3
    assert not msg1 < msg1
    assert not msg3 < msg4  # key is less but only timestamps compared


def test_compact_messages_drops_all_but_latest_for_each_key() -> None:
    msg1 = Message(timestamp=1, key="key1", value="value1")
    msg2 = Message(timestamp=2, key="key1", value="value2")
    msg3 = Message(timestamp=1, key="key0", value="value3")
    msg4 = Message(timestamp=3, key="key1", value="value3")
    assert compact_messages([msg1, msg2, msg3, msg4]) == sorted([msg3, msg4])
    assert compact_messages([msg1, msg2, msg3]) == sorted([msg2, msg3])
    assert compact_messages([msg1, msg2]) == [msg2]
    assert compact_messages([msg1]) == [msg1]
    assert not compact_messages([])
    assert compact_messages([msg1, msg2, msg1, msg2]) == [msg2]
