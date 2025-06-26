# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import pytest
import scipp as sc

from beamlime.dashboard.data_key import DataKey, MonitorDataKey
from beamlime.dashboard.data_service import DataService
from beamlime.dashboard.data_subscriber import Pipe, StreamAssembler
from beamlime.dashboard.stream_manager import (
    MonitorStreamManager,
    ReductionStreamManager,
    StreamManager,
)


class FakePipe(Pipe):
    """Fake implementation of Pipe for testing."""

    def __init__(self) -> None:
        self.send_calls: list[Any] = []
        self.data: Any = None

    def send(self, data: Any) -> None:
        self.send_calls.append(data)
        self.data = data


class FakeStreamAssembler(StreamAssembler):
    """Fake implementation of StreamAssembler for testing."""

    def __init__(self, keys: set[Any]) -> None:
        super().__init__(keys)
        self.assemble_calls: list[dict[Any, Any]] = []

    def assemble(self, data: dict[Any, Any]) -> Any:
        self.assemble_calls.append(data.copy())
        return data


class FakePipeFactory:
    """Fake pipe factory for testing."""

    def __init__(self) -> None:
        self.call_count = 0
        self.created_pipes: list[FakePipe] = []

    def __call__(self) -> FakePipe:
        self.call_count += 1
        pipe = FakePipe()
        self.created_pipes.append(pipe)
        return pipe


@pytest.fixture
def data_service() -> DataService:
    """Real DataService instance for testing."""
    return DataService()


@pytest.fixture
def fake_pipe_factory() -> FakePipeFactory:
    """Fake pipe factory that creates FakePipe instances."""
    return FakePipeFactory()


@pytest.fixture
def sample_data() -> sc.DataArray:
    """Sample data array for testing."""
    return sc.DataArray(
        data=sc.array(dims=['x'], values=[1, 2, 3]),
        coords={'x': sc.array(dims=['x'], values=[10, 20, 30])},
    )


class TestStreamManager:
    """Test cases for base StreamManager class."""

    def test_init_stores_data_service_and_pipe_factory(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that initialization stores data service and pipe factory."""
        manager = StreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        assert manager.data_service is data_service


class TestMonitorStreamManager:
    """Test cases for MonitorStreamManager class."""

    def test_get_stream_creates_correct_data_key_and_assembler(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that get_stream creates correct data key and assembler."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream('test_component')

        assert isinstance(pipe, FakePipe)
        assert fake_pipe_factory.call_count == 1

    def test_get_stream_same_component_returns_same_pipe(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that multiple calls with same component return same pipe."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe1 = manager.get_stream('test_component')
        pipe2 = manager.get_stream('test_component')

        assert pipe1 is pipe2
        assert fake_pipe_factory.call_count == 1

    def test_get_stream_different_components_create_different_pipes(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that different components create different pipes."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe1 = manager.get_stream('component1')
        pipe2 = manager.get_stream('component2')

        assert pipe1 is not pipe2
        assert fake_pipe_factory.call_count == 2

    def test_data_flow_through_monitor_stream(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that data flows through the monitor stream correctly."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream('test_component')
        monitor_key = MonitorDataKey(component_name='test_component', view_name='')

        # Set data in the service using the correct underlying data keys
        with data_service.transaction():
            data_service[monitor_key.cumulative_key()] = sample_data
            data_service[monitor_key.current_key()] = sample_data * 2

        # Verify data was sent through the pipe
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        # ComponentStreamAssembler returns RawData object
        assert hasattr(sent_data, 'cumulative')
        assert hasattr(sent_data, 'current')
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sc.identical(sent_data.current, sample_data * 2)

    def test_multiple_data_updates_through_monitor_stream(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test multiple data updates through monitor stream."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream('test_component')
        monitor_key = MonitorDataKey(component_name='test_component', view_name='')

        # Set data first time
        with data_service.transaction():
            data_service[monitor_key.cumulative_key()] = sample_data
            data_service[monitor_key.current_key()] = sample_data * 2

        # Set data second time with modified values
        modified_data = sample_data * 3
        with data_service.transaction():
            data_service[monitor_key.cumulative_key()] = modified_data
            data_service[monitor_key.current_key()] = modified_data * 2

        # Verify both updates were sent
        assert len(pipe.send_calls) == 2
        # Check first update
        first_data = pipe.send_calls[0]
        assert sc.identical(first_data.cumulative, sample_data)
        assert sc.identical(first_data.current, sample_data * 2)
        # Check second update
        second_data = pipe.send_calls[1]
        assert sc.identical(second_data.cumulative, modified_data)
        assert sc.identical(second_data.current, modified_data * 2)

    def test_partial_key_updates_trigger_monitor_stream(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that partial key updates trigger monitor stream with available data."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream('test_component')
        monitor_key = MonitorDataKey(component_name='test_component', view_name='')

        # Set only cumulative data first
        data_service[monitor_key.cumulative_key()] = sample_data

        # Verify pipe was triggered with partial data
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert hasattr(sent_data, 'cumulative')
        assert hasattr(sent_data, 'current')
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sent_data.current is None

        # Now set current data
        data_service[monitor_key.current_key()] = sample_data * 2

        # Verify pipe was triggered again with complete data
        assert len(pipe.send_calls) == 2
        sent_data = pipe.send_calls[1]
        assert hasattr(sent_data, 'cumulative')
        assert hasattr(sent_data, 'current')
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sc.identical(sent_data.current, sample_data * 2)

    def test_monitor_stream_triggered_on_each_key_update(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that monitor stream is triggered separately for each key update."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream('test_component')
        monitor_key = MonitorDataKey(component_name='test_component', view_name='')

        # Set cumulative data
        data_service[monitor_key.cumulative_key()] = sample_data
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sent_data.current is None

        # Set current data separately
        data_service[monitor_key.current_key()] = sample_data * 2
        assert len(pipe.send_calls) == 2
        sent_data = pipe.send_calls[1]
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sc.identical(sent_data.current, sample_data * 2)

        # Update cumulative data again
        data_service[monitor_key.cumulative_key()] = sample_data * 3
        assert len(pipe.send_calls) == 3
        sent_data = pipe.send_calls[2]
        assert sc.identical(sent_data.cumulative, sample_data * 3)
        assert sc.identical(sent_data.current, sample_data * 2)

        # Update current data again
        data_service[monitor_key.current_key()] = sample_data * 4
        assert len(pipe.send_calls) == 4
        sent_data = pipe.send_calls[3]
        assert sc.identical(sent_data.cumulative, sample_data * 3)
        assert sc.identical(sent_data.current, sample_data * 4)

    def test_monitor_stream_with_missing_keys_assembles_available_data(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that monitor stream assembles data even when some keys are missing."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream('test_component')
        monitor_key = MonitorDataKey(component_name='test_component', view_name='')

        # Set only one key and verify assembler is called with partial data
        data_service[monitor_key.cumulative_key()] = sample_data

        # The assembler should be called even though current_key is missing
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sent_data.current is None

    def test_multiple_monitor_streams_independent_triggering(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that multiple monitor streams are triggered independently."""
        manager = MonitorStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe1 = manager.get_stream('component1')
        pipe2 = manager.get_stream('component2')

        monitor_key1 = MonitorDataKey(component_name='component1', view_name='')
        monitor_key2 = MonitorDataKey(component_name='component2', view_name='')

        # Update component1 data
        data_service[monitor_key1.cumulative_key()] = sample_data

        # Only pipe1 should be triggered
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 0
        sent_data = pipe1.send_calls[0]
        assert sc.identical(sent_data.cumulative, sample_data)
        assert sent_data.current is None

        # Update component2 data
        data_service[monitor_key2.current_key()] = sample_data * 2

        # Only pipe2 should be triggered
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 1
        sent_data = pipe2.send_calls[0]
        assert sent_data.cumulative is None
        assert sc.identical(sent_data.current, sample_data * 2)


class TestReductionStreamManager:
    """Test cases for ReductionStreamManager class."""

    def test_get_stream_creates_correct_data_keys_and_assembler(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that get_stream creates correct data keys and assembler."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1', 'source2'}
        view_name = 'test_view'

        pipe = manager.get_stream(source_names, view_name)

        assert isinstance(pipe, FakePipe)
        assert fake_pipe_factory.call_count == 1

    def test_get_stream_same_parameters_returns_same_pipe(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that multiple calls with same parameters return same pipe."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1', 'source2'}
        view_name = 'test_view'

        pipe1 = manager.get_stream(source_names, view_name)
        pipe2 = manager.get_stream(source_names, view_name)

        assert pipe1 is pipe2
        assert fake_pipe_factory.call_count == 1

    def test_get_stream_different_source_names_order_same_pipe(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that different order of source names returns same pipe."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe1 = manager.get_stream({'source1', 'source2'}, 'test_view')
        pipe2 = manager.get_stream({'source2', 'source1'}, 'test_view')

        assert pipe1 is pipe2
        assert fake_pipe_factory.call_count == 1

    def test_get_stream_different_view_names_create_different_pipes(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that different view names create different pipes."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1', 'source2'}

        pipe1 = manager.get_stream(source_names, 'view1')
        pipe2 = manager.get_stream(source_names, 'view2')

        assert pipe1 is not pipe2
        assert fake_pipe_factory.call_count == 2

    def test_get_stream_different_source_sets_create_different_pipes(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test that different source sets create different pipes."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        view_name = 'test_view'

        pipe1 = manager.get_stream({'source1', 'source2'}, view_name)
        pipe2 = manager.get_stream({'source1', 'source3'}, view_name)

        assert pipe1 is not pipe2
        assert fake_pipe_factory.call_count == 2

    def test_data_flow_through_reduction_stream_single_source(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test data flow through reduction stream with single source."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1'}
        view_name = 'test_view'

        pipe = manager.get_stream(source_names, view_name)
        data_key = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )

        # Set data in the service
        data_service[data_key] = sample_data

        # Verify data was sent through the pipe
        assert len(pipe.send_calls) == 1
        # The assembler merges data, so it should be a dict
        sent_data = pipe.send_calls[0]
        assert isinstance(sent_data, dict)
        assert data_key in sent_data
        assert sc.identical(sent_data[data_key], sample_data)

    def test_data_flow_through_reduction_stream_multiple_sources(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test data flow through reduction stream with multiple sources."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1', 'source2'}
        view_name = 'test_view'

        pipe = manager.get_stream(source_names, view_name)

        data_key1 = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )
        data_key2 = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/test_view',
        )

        # Set data for both sources using transaction for atomic update
        with data_service.transaction():
            data_service[data_key1] = sample_data
            data_service[data_key2] = sample_data * 2

        # Verify merged data was sent through the pipe
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert isinstance(sent_data, dict)
        assert data_key1 in sent_data
        assert data_key2 in sent_data
        assert sc.identical(sent_data[data_key1], sample_data)
        assert sc.identical(sent_data[data_key2], sample_data * 2)

    def test_partial_data_updates_in_reduction_stream(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test partial data updates in reduction stream."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1', 'source2'}
        view_name = 'test_view'

        pipe = manager.get_stream(source_names, view_name)

        data_key1 = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )
        data_key2 = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/test_view',
        )

        # Set data for first source only
        data_service[data_key1] = sample_data

        # Verify partial data was sent
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert isinstance(sent_data, dict)
        assert data_key1 in sent_data
        assert data_key2 not in sent_data

        # Add second source data
        data_service[data_key2] = sample_data * 2

        # Verify complete data was sent
        assert len(pipe.send_calls) == 2
        sent_data = pipe.send_calls[1]
        assert data_key1 in sent_data
        assert data_key2 in sent_data

    def test_single_source_reduction_stream(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test reduction stream with single source."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream({'source1'}, 'test_view')

        assert isinstance(pipe, FakePipe)
        assert fake_pipe_factory.call_count == 1

    def test_empty_source_names_set(
        self, data_service: DataService, fake_pipe_factory: FakePipeFactory
    ) -> None:
        """Test reduction stream with empty source names set."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream(set(), 'test_view')

        assert isinstance(pipe, FakePipe)
        assert fake_pipe_factory.call_count == 1

    def test_reduction_stream_triggered_on_individual_source_updates(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that reduction stream is triggered on each individual source update."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1', 'source2', 'source3'}
        view_name = 'test_view'

        pipe = manager.get_stream(source_names, view_name)

        data_key1 = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )
        data_key2 = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/test_view',
        )
        data_key3 = DataKey(
            service_name='data_reduction',
            source_name='source3',
            key='reduced/source3/test_view',
        )

        # Update each source separately and verify triggering
        data_service[data_key1] = sample_data
        assert len(pipe.send_calls) == 1
        sent_data = pipe.send_calls[0]
        assert data_key1 in sent_data
        assert data_key2 not in sent_data
        assert data_key3 not in sent_data

        data_service[data_key2] = sample_data * 2
        assert len(pipe.send_calls) == 2
        sent_data = pipe.send_calls[1]
        assert data_key1 in sent_data
        assert data_key2 in sent_data
        assert data_key3 not in sent_data

        data_service[data_key3] = sample_data * 3
        assert len(pipe.send_calls) == 3
        sent_data = pipe.send_calls[2]
        assert data_key1 in sent_data
        assert data_key2 in sent_data
        assert data_key3 in sent_data

    def test_reduction_stream_partial_updates_preserve_existing_data(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that partial updates preserve existing data in reduction stream."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )
        source_names = {'source1', 'source2'}
        view_name = 'test_view'

        pipe = manager.get_stream(source_names, view_name)

        data_key1 = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/test_view',
        )
        data_key2 = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/test_view',
        )

        # Set both sources initially
        with data_service.transaction():
            data_service[data_key1] = sample_data
            data_service[data_key2] = sample_data * 2

        assert len(pipe.send_calls) == 1
        initial_data = pipe.send_calls[0]
        assert data_key1 in initial_data
        assert data_key2 in initial_data

        # Update only source1
        data_service[data_key1] = sample_data * 3

        # Verify both sources are still in the sent data
        assert len(pipe.send_calls) == 2
        updated_data = pipe.send_calls[1]
        assert data_key1 in updated_data
        assert data_key2 in updated_data
        assert sc.identical(updated_data[data_key1], sample_data * 3)
        assert sc.identical(updated_data[data_key2], sample_data * 2)

    def test_multiple_reduction_streams_independent_triggering(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that multiple reduction streams are triggered independently."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe1 = manager.get_stream({'source1'}, 'view1')
        pipe2 = manager.get_stream({'source2'}, 'view2')

        data_key1 = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/view1',
        )
        data_key2 = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/view2',
        )

        # Update source1/view1
        data_service[data_key1] = sample_data

        # Only pipe1 should be triggered
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 0

        # Update source2/view2
        data_service[data_key2] = sample_data * 2

        # Only pipe2 should be triggered
        assert len(pipe1.send_calls) == 1
        assert len(pipe2.send_calls) == 1

    def test_reduction_stream_unrelated_key_updates_dont_trigger(
        self,
        data_service: DataService,
        fake_pipe_factory: FakePipeFactory,
        sample_data: sc.DataArray,
    ) -> None:
        """Test that unrelated key updates don't trigger reduction streams."""
        manager = ReductionStreamManager(
            data_service=data_service, pipe_factory=fake_pipe_factory
        )

        pipe = manager.get_stream({'source1'}, 'view1')

        unrelated_key = DataKey(
            service_name='data_reduction',
            source_name='source2',
            key='reduced/source2/view1',
        )

        # Update unrelated key
        data_service[unrelated_key] = sample_data

        # Pipe should not be triggered
        assert len(pipe.send_calls) == 0

        # Update the correct key
        correct_key = DataKey(
            service_name='data_reduction',
            source_name='source1',
            key='reduced/source1/view1',
        )
        data_service[correct_key] = sample_data

        # Now pipe should be triggered
        assert len(pipe.send_calls) == 1
