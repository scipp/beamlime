# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.dashboard.data_key import DataKey, DetectorDataKey, MonitorDataKey


class TestComponentDataKey:
    """Test the abstract ComponentDataKey functionality."""

    def test_monitor_data_key_service_name(self) -> None:
        key = MonitorDataKey(component_name="monitor1", view_name="intensity")
        assert key.service_name == "monitor_data"

    def test_detector_data_key_service_name(self) -> None:
        key = DetectorDataKey(component_name="detector1", view_name="counts")
        assert key.service_name == "detector_data"


class TestMonitorDataKey:
    """Test MonitorDataKey specific functionality."""

    @pytest.fixture
    def monitor_key(self) -> MonitorDataKey:
        return MonitorDataKey(component_name="monitor1", view_name="intensity")

    def test_service_name(self, monitor_key: MonitorDataKey) -> None:
        assert monitor_key.service_name == "monitor_data"

    def test_cumulative_key(self, monitor_key: MonitorDataKey) -> None:
        cumulative_key = monitor_key.cumulative_key()
        expected = DataKey(
            service_name="monitor_data",
            source_name="monitor1",
            key="intensity/cumulative",
        )
        assert cumulative_key == expected

    def test_current_key(self, monitor_key: MonitorDataKey) -> None:
        current_key = monitor_key.current_key()
        expected = DataKey(
            service_name="monitor_data", source_name="monitor1", key="intensity/current"
        )
        assert current_key == expected

    def test_different_view_names(self) -> None:
        key1 = MonitorDataKey(component_name="monitor1", view_name="rate")
        key2 = MonitorDataKey(component_name="monitor1", view_name="total")

        assert key1.cumulative_key().key == "rate/cumulative"
        assert key2.cumulative_key().key == "total/cumulative"
        assert key1.current_key().key == "rate/current"
        assert key2.current_key().key == "total/current"

    def test_different_component_names(self) -> None:
        key1 = MonitorDataKey(component_name="monitor1", view_name="intensity")
        key2 = MonitorDataKey(component_name="monitor2", view_name="intensity")

        assert key1.cumulative_key().source_name == "monitor1"
        assert key2.cumulative_key().source_name == "monitor2"
        assert key1.current_key().source_name == "monitor1"
        assert key2.current_key().source_name == "monitor2"


class TestDetectorDataKey:
    """Test DetectorDataKey specific functionality."""

    @pytest.fixture
    def detector_key(self) -> DetectorDataKey:
        return DetectorDataKey(component_name="detector1", view_name="counts")

    def test_service_name(self, detector_key: DetectorDataKey) -> None:
        assert detector_key.service_name == "detector_data"

    def test_cumulative_key(self, detector_key: DetectorDataKey) -> None:
        cumulative_key = detector_key.cumulative_key()
        expected = DataKey(
            service_name="detector_data",
            source_name="detector1",
            key="counts/cumulative",
        )
        assert cumulative_key == expected

    def test_current_key(self, detector_key: DetectorDataKey) -> None:
        current_key = detector_key.current_key()
        expected = DataKey(
            service_name="detector_data", source_name="detector1", key="counts/current"
        )
        assert current_key == expected

    def test_different_view_names(self) -> None:
        key1 = DetectorDataKey(component_name="detector1", view_name="histogram")
        key2 = DetectorDataKey(component_name="detector1", view_name="spectrum")

        assert key1.cumulative_key().key == "histogram/cumulative"
        assert key2.cumulative_key().key == "spectrum/cumulative"
        assert key1.current_key().key == "histogram/current"
        assert key2.current_key().key == "spectrum/current"

    def test_different_component_names(self) -> None:
        key1 = DetectorDataKey(component_name="detector1", view_name="counts")
        key2 = DetectorDataKey(component_name="detector2", view_name="counts")

        assert key1.cumulative_key().source_name == "detector1"
        assert key2.cumulative_key().source_name == "detector2"
        assert key1.current_key().source_name == "detector1"
        assert key2.current_key().source_name == "detector2"


class TestDataKeyEquality:
    """Test DataKey equality and hashing behavior."""

    def test_data_key_equality(self) -> None:
        key1 = DataKey(service_name="test", source_name="source1", key="key1")
        key2 = DataKey(service_name="test", source_name="source1", key="key1")
        key3 = DataKey(service_name="test", source_name="source1", key="key2")

        assert key1 == key2
        assert key1 != key3

    def test_component_data_key_equality(self) -> None:
        monitor1 = MonitorDataKey(component_name="monitor1", view_name="intensity")
        monitor2 = MonitorDataKey(component_name="monitor1", view_name="intensity")
        monitor3 = MonitorDataKey(component_name="monitor2", view_name="intensity")

        assert monitor1 == monitor2
        assert monitor1 != monitor3

    def test_cross_type_inequality(self) -> None:
        monitor_key = MonitorDataKey(component_name="component1", view_name="view1")
        detector_key = DetectorDataKey(component_name="component1", view_name="view1")

        assert monitor_key != detector_key
        assert monitor_key.cumulative_key() != detector_key.cumulative_key()
