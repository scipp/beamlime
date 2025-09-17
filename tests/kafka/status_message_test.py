# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import json
import uuid

import pytest
from streaming_data_types import deserialise_x5f2, serialise_x5f2

from beamlime.config.workflow_spec import JobId, WorkflowId
from beamlime.core.job import JobState, JobStatus
from beamlime.kafka.x5f2_compat import (
    Message,
    NicosStatus,
    ServiceId,
    StatusJSON,
    StatusMessage,
    job_state_to_nicos_status_constant,
    job_status_to_x5f2,
    x5f2_to_job_status,
)


class TestServiceId:
    def test_from_string_valid_format(self):
        """Test parsing valid service_id string."""
        job_number = uuid.uuid4()
        service_id_str = f"detector_1:{job_number}"
        service_id = ServiceId.from_string(service_id_str)

        assert service_id.job_id.source_name == "detector_1"
        assert service_id.job_id.job_number == job_number

    def test_from_string_invalid_format_no_colon(self):
        """Test error handling for service_id without colon."""
        with pytest.raises(ValueError, match="Invalid service_id format"):
            ServiceId.from_string("detector_1")

    def test_from_string_invalid_format_multiple_colons(self):
        """Test parsing service_id with multiple colons (should work)."""
        job_number = uuid.uuid4()
        service_id_str = f"detector:group:1:{job_number}"
        service_id = ServiceId.from_string(service_id_str)

        assert service_id.job_id.source_name == "detector:group:1"
        assert service_id.job_id.job_number == job_number

    def test_from_string_invalid_uuid(self):
        """Test error handling for invalid UUID in job_number."""
        with pytest.raises(ValueError, match="Invalid service_id format"):
            ServiceId.from_string("detector_1:not-a-uuid")

    def test_from_job_id(self):
        """Test creating ServiceId from JobId."""
        job_id = JobId(source_name="detector_2", job_number=uuid.uuid4())
        service_id = ServiceId.from_job_id(job_id)

        assert service_id.job_id == job_id

    def test_to_string(self):
        """Test converting ServiceId to string format."""
        job_number = uuid.uuid4()
        job_id = JobId(source_name="detector_3", job_number=job_number)
        service_id = ServiceId.from_job_id(job_id)

        expected = f"detector_3:{job_number}"
        assert service_id.to_string() == expected
        assert str(service_id) == expected

    def test_round_trip_string_conversion(self):
        """Test that string conversion is reversible."""
        original_job_id = JobId(source_name="test_detector", job_number=uuid.uuid4())
        service_id = ServiceId.from_job_id(original_job_id)

        # Convert to string and back
        service_id_str = service_id.to_string()
        parsed_service_id = ServiceId.from_string(service_id_str)

        assert parsed_service_id.job_id == original_job_id


class TestJobStateToNicosStatus:
    def test_all_job_states_mapped(self):
        """Test that all JobState values have corresponding NicosStatus."""
        for state in JobState:
            status = job_state_to_nicos_status_constant(state)
            assert isinstance(status, NicosStatus)

    def test_specific_mappings(self):
        """Test specific state mappings."""
        assert job_state_to_nicos_status_constant(JobState.active) == NicosStatus.OK
        assert job_state_to_nicos_status_constant(JobState.error) == NicosStatus.ERROR
        assert job_state_to_nicos_status_constant(JobState.finishing) == NicosStatus.OK
        assert (
            job_state_to_nicos_status_constant(JobState.paused) == NicosStatus.DISABLED
        )
        assert (
            job_state_to_nicos_status_constant(JobState.scheduled)
            == NicosStatus.DISABLED
        )
        assert (
            job_state_to_nicos_status_constant(JobState.warning) == NicosStatus.WARNING
        )


class TestMessage:
    def test_message_creation_minimal(self):
        """Test creating Message with minimal required fields."""
        job_id = JobId(source_name="test", job_number=uuid.uuid4())
        message = Message(
            state=JobState.active,
            job_id=job_id,
            workflow_id="instrument/namespace/workflow/1",
        )

        assert message.state == JobState.active
        assert message.job_id == job_id
        assert message.workflow_id == "instrument/namespace/workflow/1"
        assert message.warning is None
        assert message.error is None
        assert message.start_time is None
        assert message.end_time is None

    def test_message_creation_complete(self):
        """Test creating Message with all fields."""
        job_id = JobId(source_name="test", job_number=uuid.uuid4())
        message = Message(
            state=JobState.warning,
            warning="Test warning",
            error="Test error",
            job_id=job_id,
            workflow_id="instrument/namespace/workflow/1",
            start_time=1000000000,
            end_time=2000000000,
        )

        assert message.state == JobState.warning
        assert message.warning == "Test warning"
        assert message.error == "Test error"
        assert message.start_time == 1000000000
        assert message.end_time == 2000000000


class TestStatusMessage:
    def create_sample_job_status(self, **overrides):
        """Helper to create JobStatus with defaults that can be overridden."""
        defaults = {
            "job_id": JobId(source_name="detector_1", job_number=uuid.uuid4()),
            "workflow_id": WorkflowId(
                instrument="test_inst",
                namespace="data_reduction",
                name="test_workflow",
                version=1,
            ),
            "state": JobState.active,
            "error_message": None,
            "warning_message": None,
            "start_time": None,
            "end_time": None,
        }
        defaults.update(overrides)
        return JobStatus(**defaults)

    def test_from_job_status_minimal(self):
        """Test converting minimal JobStatus to StatusMessage."""
        job_status = self.create_sample_job_status()
        status_msg = StatusMessage.from_job_status(job_status)

        assert status_msg.software_name == "beamlime"
        assert status_msg.software_version == "0.0.0"
        assert status_msg.service_id.job_id == job_status.job_id
        assert status_msg.host_name == ""
        assert status_msg.process_id == 0
        assert status_msg.update_interval == 1000

        assert status_msg.status_json.status == NicosStatus.OK
        assert status_msg.status_json.message.state == JobState.active
        assert status_msg.status_json.message.job_id == job_status.job_id
        assert status_msg.status_json.message.workflow_id == str(job_status.workflow_id)

    def test_from_job_status_with_error(self):
        """Test converting JobStatus with error to StatusMessage."""
        job_status = self.create_sample_job_status(
            state=JobState.error, error_message="Test error message"
        )
        status_msg = StatusMessage.from_job_status(job_status)

        assert status_msg.status_json.status == NicosStatus.ERROR
        assert status_msg.status_json.message.state == JobState.error
        assert status_msg.status_json.message.error == "Test error message"

    def test_from_job_status_with_warning(self):
        """Test converting JobStatus with warning to StatusMessage."""
        job_status = self.create_sample_job_status(
            state=JobState.warning, warning_message="Test warning message"
        )
        status_msg = StatusMessage.from_job_status(job_status)

        assert status_msg.status_json.status == NicosStatus.WARNING
        assert status_msg.status_json.message.state == JobState.warning
        assert status_msg.status_json.message.warning == "Test warning message"

    def test_from_job_status_with_times(self):
        """Test converting JobStatus with start/end times to StatusMessage."""
        job_status = self.create_sample_job_status(
            start_time=1000000000, end_time=2000000000
        )
        status_msg = StatusMessage.from_job_status(job_status)

        assert status_msg.status_json.message.start_time == 1000000000
        assert status_msg.status_json.message.end_time == 2000000000

    def test_to_job_status_minimal(self):
        """Test converting minimal StatusMessage to JobStatus."""
        job_id = JobId(source_name="test", job_number=uuid.uuid4())
        workflow_id = WorkflowId(
            instrument="test", namespace="ns", name="wf", version=1
        )

        status_msg = StatusMessage(
            service_id=ServiceId.from_job_id(job_id),
            status_json=StatusJSON(
                status=NicosStatus.OK,
                message=Message(
                    state=JobState.active, job_id=job_id, workflow_id=str(workflow_id)
                ),
            ),
        )

        job_status = status_msg.to_job_status()

        assert job_status.job_id == job_id
        assert job_status.workflow_id == workflow_id
        assert job_status.state == JobState.active
        assert job_status.error_message is None
        assert job_status.warning_message is None
        assert job_status.start_time is None
        assert job_status.end_time is None

    def test_to_job_status_complete(self):
        """Test converting complete StatusMessage to JobStatus."""
        job_id = JobId(source_name="test", job_number=uuid.uuid4())
        workflow_id = WorkflowId(
            instrument="test", namespace="ns", name="wf", version=1
        )

        status_msg = StatusMessage(
            service_id=ServiceId.from_job_id(job_id),
            status_json=StatusJSON(
                status=NicosStatus.WARNING,
                message=Message(
                    state=JobState.warning,
                    warning="Test warning",
                    error="Test error",
                    job_id=job_id,
                    workflow_id=str(workflow_id),
                    start_time=1000000000,
                    end_time=2000000000,
                ),
            ),
        )

        job_status = status_msg.to_job_status()

        assert job_status.job_id == job_id
        assert job_status.workflow_id == workflow_id
        assert job_status.state == JobState.warning
        assert job_status.error_message == "Test error"
        assert job_status.warning_message == "Test warning"
        assert job_status.start_time == 1000000000
        assert job_status.end_time == 2000000000

    def test_service_id_validation_from_string(self):
        """Test that service_id field accepts string input and converts to ServiceId."""
        job_number = uuid.uuid4()
        service_id_str = f"detector_1:{job_number}"

        # Create StatusMessage with string service_id
        data = {
            "service_id": service_id_str,
            "status_json": {
                "status": NicosStatus.OK,
                "message": {
                    "state": JobState.active,
                    "job_id": {"source_name": "detector_1", "job_number": job_number},
                    "workflow_id": "inst/ns/wf/1",
                },
            },
        }

        status_msg = StatusMessage.model_validate(data)
        assert isinstance(status_msg.service_id, ServiceId)
        assert status_msg.service_id.job_id.source_name == "detector_1"
        assert status_msg.service_id.job_id.job_number == job_number

    def test_service_id_serialization(self):
        """Test that service_id is serialized to string format."""
        job_id = JobId(source_name="test", job_number=uuid.uuid4())
        status_msg = StatusMessage(
            service_id=ServiceId.from_job_id(job_id),
            status_json=StatusJSON(
                status=NicosStatus.OK,
                message=Message(
                    state=JobState.active, job_id=job_id, workflow_id="inst/ns/wf/1"
                ),
            ),
        )

        # Serialize to dict and check service_id format
        data = status_msg.model_dump()
        expected_service_id = f"{job_id.source_name}:{job_id.job_number}"
        assert data["service_id"] == expected_service_id


class TestRoundTripConversion:
    """Test round-trip conversion between JobStatus and StatusMessage."""

    def create_sample_job_status(self, **overrides):
        """Helper to create JobStatus with defaults that can be overridden."""
        defaults = {
            "job_id": JobId(source_name="detector_1", job_number=uuid.uuid4()),
            "workflow_id": WorkflowId(
                instrument="test_inst",
                namespace="data_reduction",
                name="test_workflow",
                version=1,
            ),
            "state": JobState.active,
            "error_message": None,
            "warning_message": None,
            "start_time": None,
            "end_time": None,
        }
        defaults.update(overrides)
        return JobStatus(**defaults)

    def test_round_trip_minimal_job_status(self):
        """Test round-trip conversion of minimal JobStatus."""
        original = self.create_sample_job_status()

        # Convert to StatusMessage and back
        status_msg = StatusMessage.from_job_status(original)
        converted = status_msg.to_job_status()

        assert converted.job_id == original.job_id
        assert converted.workflow_id == original.workflow_id
        assert converted.state == original.state
        assert converted.error_message == original.error_message
        assert converted.warning_message == original.warning_message
        assert converted.start_time == original.start_time
        assert converted.end_time == original.end_time

    def test_round_trip_all_job_states(self):
        """Test round-trip conversion for all JobState values."""
        for state in JobState:
            original = self.create_sample_job_status(state=state)

            status_msg = StatusMessage.from_job_status(original)
            converted = status_msg.to_job_status()

            assert converted.state == original.state, f"Failed for state {state}"

    def test_round_trip_with_error_message(self):
        """Test round-trip conversion with error message."""
        original = self.create_sample_job_status(
            state=JobState.error, error_message="Critical error occurred"
        )

        status_msg = StatusMessage.from_job_status(original)
        converted = status_msg.to_job_status()

        assert converted.error_message == original.error_message
        assert converted.state == original.state

    def test_round_trip_with_warning_message(self):
        """Test round-trip conversion with warning message."""
        original = self.create_sample_job_status(
            state=JobState.warning, warning_message="Warning: potential issue detected"
        )

        status_msg = StatusMessage.from_job_status(original)
        converted = status_msg.to_job_status()

        assert converted.warning_message == original.warning_message
        assert converted.state == original.state

    def test_round_trip_with_both_messages(self):
        """Test round-trip conversion with both error and warning messages."""
        original = self.create_sample_job_status(
            state=JobState.error,
            error_message="Error occurred",
            warning_message="Warning was issued earlier",
        )

        status_msg = StatusMessage.from_job_status(original)
        converted = status_msg.to_job_status()

        assert converted.error_message == original.error_message
        assert converted.warning_message == original.warning_message
        assert converted.state == original.state

    def test_round_trip_with_timestamps(self):
        """Test round-trip conversion with start and end times."""
        original = self.create_sample_job_status(
            start_time=1000000000, end_time=2000000000
        )

        status_msg = StatusMessage.from_job_status(original)
        converted = status_msg.to_job_status()

        assert converted.start_time == original.start_time
        assert converted.end_time == original.end_time

    def test_round_trip_complex_workflow_id(self):
        """Test round-trip conversion with complex WorkflowId."""
        workflow_id = WorkflowId(
            instrument="complex-instrument-name",
            namespace="special_namespace",
            name="workflow_with_underscores",
            version=42,
        )
        original = self.create_sample_job_status(workflow_id=workflow_id)

        status_msg = StatusMessage.from_job_status(original)
        converted = status_msg.to_job_status()

        assert converted.workflow_id == original.workflow_id

    def test_round_trip_complex_job_id(self):
        """Test round-trip conversion with complex JobId."""
        job_id = JobId(source_name="complex:source:name", job_number=uuid.uuid4())
        original = self.create_sample_job_status(job_id=job_id)

        status_msg = StatusMessage.from_job_status(original)
        converted = status_msg.to_job_status()

        assert converted.job_id == original.job_id


class TestX5F2Integration:
    """Test integration with x5f2 serialization/deserialization."""

    def create_sample_job_status(self, **overrides):
        """Helper to create JobStatus with defaults that can be overridden."""
        defaults = {
            "job_id": JobId(source_name="detector_1", job_number=uuid.uuid4()),
            "workflow_id": WorkflowId(
                instrument="test_inst",
                namespace="data_reduction",
                name="test_workflow",
                version=1,
            ),
            "state": JobState.active,
            "error_message": None,
            "warning_message": None,
            "start_time": None,
            "end_time": None,
        }
        defaults.update(overrides)
        return JobStatus(**defaults)

    def test_x5f2_round_trip_minimal(self):
        """Test round-trip conversion through x5f2 with minimal JobStatus."""
        original = self.create_sample_job_status()

        # Convert to x5f2 and back
        x5f2_data = job_status_to_x5f2(original)
        converted = x5f2_to_job_status(x5f2_data)

        assert converted.job_id == original.job_id
        assert converted.workflow_id == original.workflow_id
        assert converted.state == original.state
        assert converted.error_message == original.error_message
        assert converted.warning_message == original.warning_message
        assert converted.start_time == original.start_time
        assert converted.end_time == original.end_time

    def test_x5f2_round_trip_all_states(self):
        """Test x5f2 round-trip for all JobState values."""
        for state in JobState:
            original = self.create_sample_job_status(state=state)

            x5f2_data = job_status_to_x5f2(original)
            converted = x5f2_to_job_status(x5f2_data)

            assert converted.state == original.state, f"Failed for state {state}"

    def test_x5f2_round_trip_with_messages(self):
        """Test x5f2 round-trip with error and warning messages."""
        original = self.create_sample_job_status(
            state=JobState.warning,
            error_message="Previous error",
            warning_message="Current warning",
        )

        x5f2_data = job_status_to_x5f2(original)
        converted = x5f2_to_job_status(x5f2_data)

        assert converted.error_message == original.error_message
        assert converted.warning_message == original.warning_message
        assert converted.state == original.state

    def test_x5f2_round_trip_with_timestamps(self):
        """Test x5f2 round-trip with timestamps."""
        original = self.create_sample_job_status(
            start_time=1640995200000000000,  # 2022-01-01 00:00:00 UTC in nanoseconds
            end_time=1640995800000000000,  # 2022-01-01 00:10:00 UTC in nanoseconds
        )

        x5f2_data = job_status_to_x5f2(original)
        converted = x5f2_to_job_status(x5f2_data)

        assert converted.start_time == original.start_time
        assert converted.end_time == original.end_time

    def test_x5f2_serialization_is_bytes(self):
        """Test that x5f2 serialization produces bytes."""
        job_status = self.create_sample_job_status()
        x5f2_data = job_status_to_x5f2(job_status)

        assert isinstance(x5f2_data, bytes)
        assert len(x5f2_data) > 0

    def test_x5f2_deserialization_from_bytes(self):
        """Test that x5f2 deserialization works with raw bytes."""
        job_status = self.create_sample_job_status()

        # First serialize to get reference data
        status_msg = StatusMessage.from_job_status(job_status)
        expected_data = status_msg.model_dump()

        # Serialize manually using streaming_data_types
        x5f2_data = serialise_x5f2(**expected_data)

        # Deserialize using our function
        converted = x5f2_to_job_status(x5f2_data)

        assert converted.job_id == job_status.job_id
        assert converted.workflow_id == job_status.workflow_id
        assert converted.state == job_status.state

    def test_x5f2_with_unicode_messages(self):
        """Test x5f2 handling of unicode characters in messages."""
        original = self.create_sample_job_status(
            state=JobState.error,
            error_message="Error with unicode: åäö 中文 🚀",
            warning_message="Warning with unicode: ñáéíóú",
        )

        x5f2_data = job_status_to_x5f2(original)
        converted = x5f2_to_job_status(x5f2_data)

        assert converted.error_message == original.error_message
        assert converted.warning_message == original.warning_message

    def test_x5f2_data_compatibility(self):
        """Test that x5f2 data is compatible with direct streaming_data_types usage."""
        job_status = self.create_sample_job_status(
            state=JobState.active, start_time=1000000000, end_time=2000000000
        )

        # Serialize using our function
        x5f2_data = job_status_to_x5f2(job_status)

        # Deserialize using streaming_data_types directly
        raw_data = deserialise_x5f2(x5f2_data)

        # Validate the structure - raw_data is a namedtuple with attributes
        assert hasattr(raw_data, 'service_id')
        assert hasattr(raw_data, 'status_json')
        assert hasattr(raw_data, 'software_name')
        assert raw_data.software_name == "beamlime"

        # Validate nested structure - status_json is a string that needs parsing
        status_json = json.loads(raw_data.status_json)
        assert "status" in status_json
        assert "message" in status_json

        message = status_json["message"]
        assert "state" in message
        assert "job_id" in message
        assert "workflow_id" in message
