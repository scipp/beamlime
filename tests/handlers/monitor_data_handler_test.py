# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import numpy as np
import pytest
import scipp as sc
from scipp.testing import assert_identical

from beamlime import StreamId, StreamKind
from beamlime.config.instrument import Instrument
from beamlime.config.workflow_spec import WorkflowConfig
from beamlime.handlers.accumulators import CollectTOA, Cumulative
from beamlime.handlers.monitor_data_handler import (
    MonitorDataParams,
    MonitorHandlerFactory,
    MonitorStreamProcessor,
    register_monitor_workflows,
)
from beamlime.parameter_models import TimeUnit, TOAEdges


class TestMonitorDataParams:
    def test_default_values(self):
        """Test that MonitorDataParams has correct default values."""
        params = MonitorDataParams()

        assert params.toa_edges.start == 0.0
        assert params.toa_edges.stop == 1000.0 / 14
        assert params.toa_edges.num_bins == 100
        assert params.toa_edges.unit == TimeUnit.MS

    def test_custom_values(self):
        """Test MonitorDataParams with custom values."""
        custom_edges = TOAEdges(
            start=10.0,
            stop=50.0,
            num_bins=200,
            unit=TimeUnit.US,
        )
        params = MonitorDataParams(toa_edges=custom_edges)

        assert params.toa_edges.start == 10.0
        assert params.toa_edges.stop == 50.0
        assert params.toa_edges.num_bins == 200
        assert params.toa_edges.unit == TimeUnit.US

    def test_get_edges(self):
        """Test that get_edges returns correct scipp Variable."""
        params = MonitorDataParams()
        edges = params.toa_edges.get_edges()

        assert isinstance(edges, sc.Variable)
        assert edges.unit == sc.Unit('ms')
        assert len(edges) == 101  # num_bins + 1


class TestMonitorStreamProcessor:
    @pytest.fixture
    def edges(self):
        """Create test edges."""
        return sc.linspace("tof", 0.0, 100.0, 11, unit="ms")

    @pytest.fixture
    def processor(self, edges):
        """Create MonitorStreamProcessor instance."""
        return MonitorStreamProcessor(edges)

    def test_initialization(self, edges):
        """Test MonitorStreamProcessor initialization."""
        processor = MonitorStreamProcessor(edges)
        # Test public behavior: processor should be able to accumulate data
        toa_data = np.array([10e6])  # Test with minimal data
        processor.accumulate({"det1": toa_data})
        result = processor.finalize()
        assert "cumulative" in result
        assert "current" in result

    def test_accumulate_numpy_array(self, processor):
        """Test accumulation with numpy array (from CollectTOA)."""
        # Create mock TOA data in nanoseconds
        toa_data = np.array(
            [10e6, 25e6, 45e6, 75e6, 95e6]
        )  # 10, 25, 45, 75, 95 ms in ns
        data = {"detector1": toa_data}

        processor.accumulate(data)

        # Test by finalizing and checking the result
        result = processor.finalize()
        assert "current" in result
        assert result["current"].dims == ("tof",)
        assert result["current"].unit == sc.units.counts
        # Check that events were histogrammed correctly
        assert result["current"].sum().value > 0

    def test_accumulate_scipp_dataarray(self, processor):
        """Test accumulation with scipp DataArray."""
        # Create mock histogram data
        tof_coords = sc.linspace("time", 5.0, 95.0, 10, unit="ms")
        counts = sc.ones(dims=["time"], shape=[9], unit="counts")
        hist_data = sc.DataArray(data=counts, coords={"time": tof_coords})
        data = {"detector1": hist_data}

        processor.accumulate(data)

        # Test by finalizing and checking the result
        result = processor.finalize()
        assert "current" in result
        assert result["current"].dims == ("tof",)
        assert result["current"].unit == sc.units.counts

    def test_accumulate_multiple_calls(self, processor):
        """Test multiple accumulate calls add data correctly."""
        # First accumulation
        toa_data1 = np.array([10e6, 25e6])  # 10, 25 ms in ns
        processor.accumulate({"det1": toa_data1})
        first_result = processor.finalize()
        first_sum = first_result["current"].sum().value

        # Second accumulation - need new processor since finalize clears current
        processor2 = MonitorStreamProcessor(processor._edges)
        toa_data2 = np.array([35e6, 45e6])  # 35, 45 ms in ns
        processor2.accumulate({"det1": toa_data2})
        second_result = processor2.finalize()
        second_sum = second_result["current"].sum().value

        # Both should have data
        assert first_sum > 0
        assert second_sum > 0

    def test_accumulate_wrong_number_of_items(self, processor):
        """Test that accumulate raises error with wrong number of data items."""
        data = {"det1": np.array([10e6]), "det2": np.array([20e6])}

        with pytest.raises(ValueError, match="exactly one data item"):
            processor.accumulate(data)

    def test_finalize_first_time(self, processor):
        """Test finalize on first call."""
        toa_data = np.array([10e6, 25e6, 45e6])
        processor.accumulate({"det1": toa_data})

        result = processor.finalize()

        assert "cumulative" in result
        assert "current" in result
        assert_identical(result["cumulative"], result["current"])

        # After finalize, we can finalize again without new data, since empty batches
        # will be committed.
        empty_result = processor.finalize()
        assert empty_result["current"].sum().value == 0
        assert (
            empty_result["cumulative"].sum().value == result["cumulative"].sum().value
        )

    def test_finalize_subsequent_calls(self, processor):
        """Test finalize accumulates over multiple calls."""
        # First round
        processor.accumulate({"det1": np.array([10e6, 25e6])})
        first_result = processor.finalize()
        first_cumulative_sum = first_result["cumulative"].sum().value

        # Second round
        processor.accumulate({"det1": np.array([35e6, 45e6])})
        second_result = processor.finalize()
        second_cumulative_sum = second_result["cumulative"].sum().value

        assert second_cumulative_sum > first_cumulative_sum
        # Current should only contain the latest data
        assert second_result["current"].sum().value < second_cumulative_sum

    def test_finalize_without_data(self, processor):
        """Test finalize raises error when no data has been added."""
        with pytest.raises(ValueError, match="No data has been added"):
            processor.finalize()

    def test_clear(self, processor):
        """Test clear method resets processor state."""
        processor.accumulate({"det1": np.array([10e6, 25e6])})
        processor.finalize()

        processor.clear()

        # After clear, should not be able to finalize without new data
        with pytest.raises(ValueError, match="No data has been added"):
            processor.finalize()

    def test_coordinate_unit_conversion(self, processor):
        """Test that coordinates are properly converted to match edges unit."""
        # Create data with different time unit
        tof_coords = sc.linspace("time", 5000.0, 95000.0, 10, unit="us")  # microseconds
        counts = sc.ones(dims=["time"], shape=[9], unit="counts")
        hist_data = sc.DataArray(data=counts, coords={"time": tof_coords})

        processor.accumulate({"det1": hist_data})
        result = processor.finalize()

        assert "current" in result
        assert result["current"].coords["tof"].unit == 'ms'


@pytest.fixture
def test_instrument():
    """Create a test instrument for testing."""
    return Instrument(name="test_instrument")


def test_make_beam_monitor_instrument(test_instrument):
    """Test register_monitor_workflows function."""
    source_names = ["source1", "source2"]

    register_monitor_workflows(test_instrument, source_names)

    factory = test_instrument.workflow_factory

    # Currently there is only one workflow registered
    assert len(factory) == 1
    id, spec = next(iter(factory.items()))
    assert id == spec.get_id()
    assert spec.name == "monitor_histogram"

    processor = factory.create(
        source_name='source1', config=WorkflowConfig(identifier=id, params={})
    )
    assert isinstance(processor, MonitorStreamProcessor)


class TestMonitorHandlerFactory:
    @pytest.fixture
    def mock_instrument(self, test_instrument):
        """Create a mock instrument for testing."""
        register_monitor_workflows(test_instrument, ["source1"])
        return test_instrument

    @pytest.fixture
    def factory(self, mock_instrument):
        """Create MonitorHandlerFactory instance."""
        return MonitorHandlerFactory(instrument=mock_instrument)

    def test_make_preprocessor_monitor_counts(self, factory):
        """Test preprocessor creation for monitor counts."""
        stream_id = StreamId(kind=StreamKind.MONITOR_COUNTS, name="test")

        preprocessor = factory.make_preprocessor(stream_id)

        assert isinstance(preprocessor, Cumulative)
        # Check that clear_on_get is True
        assert preprocessor._clear_on_get is True

    def test_make_preprocessor_monitor_events(self, factory):
        """Test preprocessor creation for monitor events."""
        stream_id = StreamId(kind=StreamKind.MONITOR_EVENTS, name="test")

        preprocessor = factory.make_preprocessor(stream_id)

        assert isinstance(preprocessor, CollectTOA)

    def test_make_preprocessor_other_kind(self, factory):
        """Test preprocessor creation for unsupported stream kind."""
        # Assuming there's another StreamKind that's not supported
        stream_id = StreamId(kind=StreamKind.DETECTOR_EVENTS, name="test")

        preprocessor = factory.make_preprocessor(stream_id)

        assert preprocessor is None

    def test_make_preprocessor_multiple_calls(self, factory):
        """Test that multiple calls create separate instances."""
        stream_id1 = StreamId(kind=StreamKind.MONITOR_COUNTS, name="test1")
        stream_id2 = StreamId(kind=StreamKind.MONITOR_COUNTS, name="test2")

        preprocessor1 = factory.make_preprocessor(stream_id1)
        preprocessor2 = factory.make_preprocessor(stream_id2)

        assert preprocessor1 is not preprocessor2
        assert type(preprocessor1) is type(preprocessor2)
