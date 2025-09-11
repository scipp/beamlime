# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from ess.livedata.config import streams
from ess.livedata.config.instruments import available_instruments


@pytest.mark.parametrize('instrument', available_instruments())
def test_get_stream_mapping_dev(instrument: str) -> None:
    stream_mapping = streams.get_stream_mapping(instrument=instrument, dev=True)
    assert stream_mapping is not None
    assert isinstance(stream_mapping, streams.StreamMapping)


@pytest.mark.parametrize('instrument', available_instruments())
def test_get_stream_mapping_production(instrument: str) -> None:
    stream_mapping = streams.get_stream_mapping(instrument=instrument, dev=False)
    assert stream_mapping is not None
    assert isinstance(stream_mapping, streams.StreamMapping)
