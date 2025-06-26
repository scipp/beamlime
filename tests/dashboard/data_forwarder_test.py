# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest
import scipp as sc

from beamlime.dashboard.data_forwarder import DataForwarder
from beamlime.dashboard.data_key import DataKey
from beamlime.dashboard.data_service import DataService


class TestDataForwarder:
    def test_init_stores_data_services(self) -> None:
        service1 = DataService()
        service2 = DataService()
        data_services = {'service1': service1, 'service2': service2}

        forwarder = DataForwarder(data_services)

        assert 'service1' in forwarder
        assert 'service2' in forwarder
        assert 'service3' not in forwarder

    def test_forward_with_valid_stream_name(self) -> None:
        service = DataService()
        data_services = {'monitor_data': service}
        forwarder = DataForwarder(data_services)

        data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))
        stream_name = 'detector1/monitor_data/cumulative'

        forwarder.forward(stream_name, data)

        expected_key = DataKey(
            service_name='monitor_data', source_name='detector1', key='cumulative'
        )
        assert expected_key in service
        assert sc.identical(service[expected_key], data)

    def test_forward_with_complex_key_suffix(self) -> None:
        service = DataService()
        data_services = {'detector_data': service}
        forwarder = DataForwarder(data_services)

        data = sc.DataArray(sc.array(dims=['y'], values=[4, 5, 6]))
        stream_name = 'source1/detector_data/view1/current/extra/path'

        forwarder.forward(stream_name, data)

        expected_key = DataKey(
            service_name='detector_data',
            source_name='source1',
            key='view1/current/extra/path',
        )
        assert expected_key in service
        assert sc.identical(service[expected_key], data)

    def test_forward_to_multiple_services(self) -> None:
        service1 = DataService()
        service2 = DataService()
        data_services = {'service1': service1, 'service2': service2}
        forwarder = DataForwarder(data_services)

        data1 = sc.DataArray(sc.array(dims=['x'], values=[1]))
        data2 = sc.DataArray(sc.array(dims=['y'], values=[2]))

        forwarder.forward('source/service1/key1', data1)
        forwarder.forward('source/service2/key2', data2)

        key1 = DataKey(service_name='service1', source_name='source', key='key1')
        key2 = DataKey(service_name='service2', source_name='source', key='key2')

        assert key1 in service1
        assert key2 in service2
        assert sc.identical(service1[key1], data1)
        assert sc.identical(service2[key2], data2)

    def test_forward_with_nonexistent_service(self) -> None:
        service = DataService()
        data_services = {'existing_service': service}
        forwarder = DataForwarder(data_services)

        data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))
        stream_name = 'source/nonexistent_service/key'

        # Should not raise an exception, just silently ignore
        forwarder.forward(stream_name, data)

        # Verify no data was added to the existing service
        assert len(service) == 0

    def test_forward_with_insufficient_stream_name_parts(self) -> None:
        service = DataService()
        data_services = {'service': service}
        forwarder = DataForwarder(data_services)

        data = sc.DataArray(sc.array(dims=['x'], values=[1]))

        # Stream name with only one part
        with pytest.raises(ValueError, match="Invalid stream name format"):
            forwarder.forward('single_part', data)

        # Stream name with only two parts
        with pytest.raises(ValueError, match="Invalid stream name format"):
            forwarder.forward('source/service', data)

    def test_forward_overwrites_existing_data(self) -> None:
        service = DataService()
        data_services = {'service': service}
        forwarder = DataForwarder(data_services)

        key = DataKey(service_name='service', source_name='source', key='key')
        original_data = sc.DataArray(sc.array(dims=['x'], values=[1, 2]))
        new_data = sc.DataArray(sc.array(dims=['y'], values=[3, 4, 5]))

        # Add initial data
        forwarder.forward('source/service/key', original_data)
        assert sc.identical(service[key], original_data)

        # Overwrite with new data
        forwarder.forward('source/service/key', new_data)
        assert sc.identical(service[key], new_data)

    def test_forward_with_empty_data_services(self) -> None:
        forwarder = DataForwarder({})

        data = sc.DataArray(sc.array(dims=['x'], values=[1]))
        stream_name = 'source/service/key'

        # Should not raise an exception
        forwarder.forward(stream_name, data)

    def test_forward_with_different_data_types(self) -> None:
        service = DataService()
        data_services = {'service': service}
        forwarder = DataForwarder(data_services)

        # Test with different scipp data types
        int_data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))
        float_data = sc.DataArray(sc.array(dims=['y'], values=[1.5, 2.5]))
        string_data = sc.DataArray(sc.array(dims=['z'], values=['a', 'b']))

        forwarder.forward('source/service/int_key', int_data)
        forwarder.forward('source/service/float_key', float_data)
        forwarder.forward('source/service/string_key', string_data)

        int_key = DataKey(service_name='service', source_name='source', key='int_key')
        float_key = DataKey(
            service_name='service', source_name='source', key='float_key'
        )
        string_key = DataKey(
            service_name='service', source_name='source', key='string_key'
        )

        assert sc.identical(service[int_key], int_data)
        assert sc.identical(service[float_key], float_data)
        assert sc.identical(service[string_key], string_data)
