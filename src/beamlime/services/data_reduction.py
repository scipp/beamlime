# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Service that runs a data reduction workflow."""

import argparse
import logging
from contextlib import ExitStack
from functools import partial
from typing import Literal, NewType, NoReturn

import sciline
import scipp as sc
from ess.reduce.streaming import StreamProcessor

from beamlime import Service
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.handlers.data_reduction_handler import ReductionHandlerFactory
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    ChainedAdapter,
    Ev44ToDetectorEventsAdapter,
    KafkaToEv44Adapter,
)
from beamlime.kafka.sink import KafkaSink, UnrollingSinkAdapter
from beamlime.service_factory import DataServiceBuilder
from beamlime.sinks import PlotToPngSink


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Detector Data Service')
    parser.add_argument(
        '--sink-type',
        choices=['kafka', 'png'],
        default='kafka',
        help='Select sink type: kafka or png',
    )
    return parser


DetectorData = NewType('DetectorData', sc.DataArray)
Mon1 = NewType('Mon1', sc.DataArray)
Mon2 = NewType('Mon2', sc.DataArray)
TransmissionFraction = NewType('TransmissionFraction', sc.DataArray)
IofQ = NewType('IofQ', sc.DataArray)


def transmission_fraction(mon1: Mon1, mon2: Mon2) -> TransmissionFraction:
    return mon1.sum() / mon2.sum()


def iofq(data: DetectorData, transmission_fraction: TransmissionFraction) -> IofQ:
    return data / transmission_fraction


wf = sciline.Pipeline((transmission_fraction, iofq))
processor = StreamProcessor(
    wf,
    dynamic_keys=(Mon1, Mon2, DetectorData),
    accumulators=(IofQ,),
    target_keys=(IofQ,),
)
# TODO using wf key for processors does not work, since several source_names can map
# to the same workflow key. Use source name here!
processors = {
    'mantle_detector': processor,
    'endcap_backward_detector': processor,
    'endcap_forward_detector': processor,
    'high_resolution_detector': processor,
}


# source_to_key = {'loki_detector_0': NeXusData[NXdetector, SampleRun]}
source_to_key = {
    'mantle_detector': DetectorData,
    'endcap_backward_detector': DetectorData,
    'endcap_forward_detector': DetectorData,
    'high_resolution_detector': DetectorData,
}


def make_reduction_service_builder(
    *, instrument: str, log_level: int = logging.INFO
) -> DataServiceBuilder:
    adapter = ChainedAdapter(
        first=KafkaToEv44Adapter(), second=Ev44ToDetectorEventsAdapter()
    )
    handler_factory_cls = partial(
        ReductionHandlerFactory, processors=processors, source_to_key=source_to_key
    )
    return DataServiceBuilder(
        instrument=instrument,
        name='data_reduction',
        log_level=log_level,
        adapter=adapter,
        handler_factory_cls=handler_factory_cls,
    )


def run_service(
    *,
    sink_type: Literal['kafka', 'png'],
    instrument: str,
    log_level: int = logging.INFO,
) -> NoReturn:
    config = load_config(namespace=config_names.detector_data, env='')
    consumer_config = load_config(namespace=config_names.raw_data_consumer, env='')
    kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
    kafka_upstream_config = load_config(namespace=config_names.kafka_upstream)

    if sink_type == 'kafka':
        sink = KafkaSink(kafka_config=kafka_downstream_config)
    else:
        sink = PlotToPngSink()
    sink = UnrollingSinkAdapter(sink)

    builder = make_reduction_service_builder(instrument=instrument, log_level=log_level)

    with ExitStack() as stack:
        control_consumer = stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=instrument)
        )
        consumer = stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=config['topics'],
                config={**consumer_config, **kafka_upstream_config},
                instrument=instrument,
                group='data_reduction',
            )
        )
        service = builder.build(
            control_consumer=control_consumer, consumer=consumer, sink=sink
        )
        service.start()


def main() -> NoReturn:
    parser = setup_arg_parser()
    run_service(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
