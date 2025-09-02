# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import uuid
from collections.abc import Hashable
from copy import deepcopy
from typing import Any

import pytest
import scipp as sc

from beamlime.config.workflow_spec import JobSchedule, WorkflowId
from beamlime.core.job import Job, JobId, JobResult, WorkflowData
from beamlime.core.message import StreamId


class TestJobResult:
    def test_stream_name(self):
        workflow_id = WorkflowId(
            instrument="TEST",
            namespace="data_reduction",
            name="test_workflow",
            version=1,
        )
        job_number = uuid.uuid4()
        job_id = JobId(source_name="test_source", job_number=job_number)
        result = JobResult(
            job_id=job_id,
            workflow_id=workflow_id,
            start_time=100,
            end_time=200,
            data=sc.DataArray(sc.scalar(3.14)),
            error_message=None,
        )
        assert result.stream_name == (
            '{"workflow_id":{"instrument":"TEST","namespace":"data_reduction",'
            '"name":"test_workflow","version":1},"job_id":{"source_name":"test_source",'
            '"job_number":"' + str(job_number) + '"},"output_name":null}'
        )

    def test_stream_name_with_output_name(self):
        workflow_id = WorkflowId(
            instrument="TEST",
            namespace="data_reduction",
            name="test_workflow",
            version=1,
        )
        job_number = uuid.uuid4()
        job_id = JobId(source_name="test_source", job_number=job_number)
        result = JobResult(
            job_id=job_id,
            workflow_id=workflow_id,
            output_name="output1",
            start_time=100,
            end_time=200,
            data=sc.DataArray(sc.scalar(3.14)),
            error_message=None,
        )
        assert result.stream_name == (
            '{"workflow_id":{"instrument":"TEST","namespace":"data_reduction",'
            '"name":"test_workflow","version":1},"job_id":{"source_name":"test_source",'
            '"job_number":"' + str(job_number) + '"},"output_name":"output1"}'
        )


class FakeProcessor:
    """Fake implementation of StreamProcessor for testing."""

    def __init__(self):
        self.data: dict[Hashable, Any] = {}
        self.accumulate_calls = []
        self.finalize_calls = 0
        self.clear_calls = 0
        self.should_fail_accumulate = False
        self.should_fail_finalize = False

    def accumulate(self, data: dict[Hashable, Any]) -> None:
        if self.should_fail_accumulate:
            raise RuntimeError("Accumulate failure")
        self.accumulate_calls.append(data.copy())
        for key, value in data.items():
            if key in self.data:
                self.data[key] += value
            else:
                self.data[key] = deepcopy(value)

    def finalize(self) -> dict[Hashable, Any]:
        if self.should_fail_finalize:
            raise RuntimeError("Finalize failure")
        self.finalize_calls += 1
        return self.data.copy()

    def clear(self) -> None:
        self.clear_calls += 1
        self.data.clear()
        self.accumulate_calls.clear()


@pytest.fixture
def fake_processor():
    return FakeProcessor()


@pytest.fixture
def sample_workflow_id():
    return WorkflowId(
        instrument="TEST",
        namespace="data_reduction",
        name="test_workflow",
        version=1,
    )


@pytest.fixture
def sample_job(fake_processor: FakeProcessor, sample_workflow_id: WorkflowId):
    job_id = JobId(source_name="test_source", job_number=1)
    return Job(
        job_id=job_id,
        workflow_id=sample_workflow_id,
        processor=fake_processor,
        source_mapping={"test_source": "main", "aux_source": "aux"},
    )


@pytest.fixture
def sample_workflow_data():
    return WorkflowData(
        start_time=100,
        end_time=200,
        data={
            StreamId(name="test_source"): sc.scalar(42.0),
            StreamId(name="aux_source"): sc.scalar(10.0),
            StreamId(name="unknown_source"): sc.scalar(99.0),  # Should be ignored
        },
    )


class TestJobSchedule:
    def test_valid_schedule_with_start_and_end(self):
        """Test creating a valid schedule with start and end times."""
        schedule = JobSchedule(start_time=100, end_time=200)
        assert schedule.start_time == 100
        assert schedule.end_time == 200

    def test_valid_schedule_with_immediate_start_and_end(self):
        """Test creating a valid schedule with immediate start (-1) and end time."""
        schedule = JobSchedule(start_time=-1, end_time=100)
        assert schedule.start_time == -1
        assert schedule.end_time == 100

    def test_valid_schedule_with_no_end_time(self):
        """Test creating a valid schedule with no end time (None)."""
        schedule = JobSchedule(start_time=100, end_time=None)
        assert schedule.start_time == 100
        assert schedule.end_time is None

    def test_valid_schedule_defaults(self):
        """Test creating a schedule with default values."""
        schedule = JobSchedule()
        assert schedule.start_time is None
        assert schedule.end_time is None

    def test_invalid_schedule_end_before_start(self):
        """Test that end_time < start_time raises ValueError."""
        with pytest.raises(
            ValueError,
            match="Job end_time=100 must be greater than start_time=200",
        ):
            JobSchedule(start_time=200, end_time=100)

    def test_invalid_schedule_end_equals_start(self):
        """Test that end_time == start_time raises ValueError."""
        with pytest.raises(
            ValueError,
            match="Job end_time=100 must be greater than start_time=100",
        ):
            JobSchedule(start_time=100, end_time=100)

    def test_valid_schedule_negative_start_times_other_than_minus_one(self):
        """Test that negative start times other than -1 are treated as regular times."""
        schedule = JobSchedule(start_time=-100, end_time=200)
        assert schedule.start_time == -100
        assert schedule.end_time == 200

    def test_invalid_schedule_negative_start_with_equal_end(self):
        """Test that negative start time (not -1) with equal end time still raises."""
        with pytest.raises(
            ValueError,
            match="Job end_time=-50 must be greater than start_time=-50",
        ):
            JobSchedule(start_time=-50, end_time=-50)


class TestJob:
    def test_initial_state(self, sample_job):
        """Test initial state of a Job."""
        assert sample_job.start_time is None
        assert sample_job.end_time is None

    def test_add_data_sets_times(self, sample_job, sample_workflow_data):
        """Test that adding data sets start and end times."""
        status = sample_job.add(sample_workflow_data)

        assert not status.has_error
        assert status.error_message is None
        assert sample_job.start_time == 100
        assert sample_job.end_time == 200

    def test_add_data_multiple_times_updates_end_time(self, sample_job):
        """Test that adding data multiple times only updates end time."""
        data1 = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        data2 = WorkflowData(
            start_time=120,
            end_time=250,
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )

        status1 = sample_job.add(data1)
        assert not status1.has_error
        assert sample_job.start_time == 100
        assert sample_job.end_time == 150

        status2 = sample_job.add(data2)
        assert not status2.has_error
        assert sample_job.start_time == 100  # Should not change
        assert sample_job.end_time == 250  # Should update

    def test_add_data_filters_by_source_mapping(self, sample_job, fake_processor):
        """Test that add() only processes data from mapped sources."""
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={
                StreamId(name="test_source"): sc.scalar(42.0),
                StreamId(name="aux_source"): sc.scalar(10.0),
                StreamId(name="unmapped_source"): sc.scalar(99.0),
            },
        )

        status = sample_job.add(data)
        assert not status.has_error

        # Check that processor received only mapped data
        assert len(fake_processor.accumulate_calls) == 1
        accumulated = fake_processor.accumulate_calls[0]
        assert "main" in accumulated
        assert "aux" in accumulated
        assert accumulated["main"] == sc.scalar(42.0)
        assert accumulated["aux"] == sc.scalar(10.0)
        # unmapped_source should not appear
        assert len(accumulated) == 2  # Only main and aux should be present

    def test_add_data_error_handling(self, fake_processor, sample_workflow_id):
        """Test error handling during data processing."""
        job_id = JobId(source_name="test_source", job_number=1)
        job = Job(
            job_id=job_id,
            workflow_id=sample_workflow_id,
            processor=fake_processor,
            source_mapping={"test_source": "main"},
        )

        # Make processor fail
        fake_processor.should_fail_accumulate = True

        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )

        status = job.add(data)
        assert status.has_error
        assert status.job_id == job_id
        assert f"Error processing data for job {job_id}" in status.error_message
        assert "Accumulate failure" in status.error_message

    def test_add_data_error_recovery(self, fake_processor, sample_workflow_id):
        """Test that job can recover after an error."""
        job_id = JobId(source_name="test_source", job_number=1)
        job = Job(
            job_id=job_id,
            workflow_id=sample_workflow_id,
            processor=fake_processor,
            source_mapping={"test_source": "main"},
        )

        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )

        # First, cause an error
        fake_processor.should_fail_accumulate = True
        status1 = job.add(data)
        assert status1.has_error

        # Then fix the processor and try again
        fake_processor.should_fail_accumulate = False
        status2 = job.add(data)
        assert not status2.has_error
        assert status2.error_message is None

        # Job should now return successful result
        result = job.get()
        assert result.error_message is None
        assert result.data is not None

    def test_get_returns_job_result(self, sample_job, sample_workflow_data):
        """Test that get() returns a proper JobResult."""
        sample_job.add(sample_workflow_data)
        result = sample_job.get()

        assert isinstance(result, JobResult)
        assert result.job_id.source_name == "test_source"
        assert result.job_id.job_number == 1
        assert result.workflow_id.name == "test_workflow"
        assert result.start_time == 100
        assert result.end_time == 200
        assert isinstance(result.data, sc.DataGroup)
        assert result.error_message is None

    def test_get_calls_processor_finalize(self, sample_job, fake_processor):
        """Test that get() calls processor.finalize()."""
        sample_job.get()
        assert fake_processor.finalize_calls == 1

    def test_get_with_finalize_error(self, fake_processor, sample_workflow_id):
        """Test get() handles finalize errors."""
        job_id = JobId(source_name="test_source", job_number=1)
        job = Job(
            job_id=job_id,
            workflow_id=sample_workflow_id,
            processor=fake_processor,
            source_mapping={"test_source": "main"},
        )

        # Add some data successfully
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        job.add(data)

        # Make finalize fail
        fake_processor.should_fail_finalize = True

        result = job.get()
        assert result.error_message is not None
        assert f"Error finalizing job {job_id}" in result.error_message
        assert "Finalize failure" in result.error_message
        assert result.data is None

    def test_reset_clears_processor_and_times(
        self, sample_job, sample_workflow_data, fake_processor
    ):
        """Test that reset() clears processor and resets times."""
        sample_job.add(sample_workflow_data)
        assert sample_job.start_time == 100
        assert sample_job.end_time == 200

        sample_job.reset()

        assert fake_processor.clear_calls == 1
        assert sample_job.start_time is None
        assert sample_job.end_time is None

    def test_can_get_after_error_in_add(self, fake_processor, sample_workflow_id):
        """Adding bad data should not directly affect get()."""
        job_id = JobId(source_name="test_source", job_number=1)
        job = Job(
            job_id=job_id,
            workflow_id=sample_workflow_id,
            processor=fake_processor,
            source_mapping={"test_source": "main"},
        )

        # Cause an error
        fake_processor.should_fail_accumulate = True
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        job.add(data)

        result = job.get()
        # No error, provided that the processor does not fail finalize
        assert result.error_message is None
