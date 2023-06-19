# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)


def test_readable_buffer_wrong_initial_data():
    import pytest

    from beamlime.communication.pipes import ReadableBuffer

    with pytest.raises(TypeError):
        ReadableBuffer(None)
