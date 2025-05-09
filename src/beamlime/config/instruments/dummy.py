# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Detector configuration for a dummy instrument used for development and testing.
"""

from typing import NewType

import sciline
import scipp as sc
from ess.reduce.streaming import StreamProcessor

from beamlime.config.env import StreamingEnv
from beamlime.handlers.stream_processor_factory import StreamProcessorFactory
from beamlime.kafka import InputStreamKey, StreamLUT, StreamMapping

from ._ess import make_common_stream_mapping_inputs, make_dev_stream_mapping

detectors_config = {
    'detectors': {
        'Panel 0': {
            'detector_name': 'panel_0',
            'detector_number': sc.arange('yx', 0, 128**2, unit=None).fold(
                dim='yx', sizes={'y': -1, 'x': 128}
            ),
        },
    },
    'fakes': {
        'panel_0': (1, 128**2),
    },
}


def _make_dummy_detectors() -> StreamLUT:
    """Dummy detector mapping."""
    return {InputStreamKey(topic='dummy_detector', source_name='panel_0'): 'panel_0'}


Events = NewType('Events', sc.DataArray)
TotalCounts = NewType('TotalCounts', sc.DataArray)


def _total_counts(events: Events) -> TotalCounts:
    """Calculate total counts from events."""
    return TotalCounts(events.sum())


_total_counts_workflow = sciline.Pipeline((_total_counts,))

processor_factory = StreamProcessorFactory()
source_to_key = {'panel_0': Events}
f144_attribute_registry = {}


@processor_factory.register(
    name='Total counts',
    description='Dummy workflow that simply computes the total counts.',
    source_names=['panel_0'],
)
def _total_counts() -> StreamProcessor:
    """Dummy processor for development and testing."""
    return StreamProcessor(
        base_workflow=_total_counts_workflow.copy(),
        dynamic_keys=(Events,),
        target_keys=(TotalCounts,),
        accumulators=(TotalCounts,),
    )


stream_mapping = {
    StreamingEnv.DEV: make_dev_stream_mapping(
        'dummy', detectors=list(detectors_config['fakes'])
    ),
    StreamingEnv.PROD: StreamMapping(
        **make_common_stream_mapping_inputs(instrument='dummy'),
        detectors=_make_dummy_detectors(),
    ),
}
