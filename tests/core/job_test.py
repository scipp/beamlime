# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import pytest
import scipp as sc
from collections.abc import Hashable
from typing import Any

from beamlime.core.job import (
    Job,
    JobId,
    JobResult,
    JobManager,
    JobFactory,
    WorkflowData,
)
from beamlime.core.message import StreamId
from beamlime.config.workflow_spec import WorkflowConfig


class FakeProcessor:
    """Fake implementation of StreamProcessor for testing."""

    def __init__(self):
        self.data: dict[Hashable, Any] = {}
        self.accumulate_calls = []
        self.finalize_calls = 0
        self.clear_calls = 0

    def accumulate(self, data: dict[Hashable, Any]) -> None:
        self.accumulate_calls.append(data.copy())
        for key, value in data.items():
            if key in self.data:
                # Simple accumulation logic for testing
                if isinstance(value, (int, float)) and isinstance(
                    self.data[key], (int, float)
                ):
                    self.data[key] += value
                else:
                    self.data[key] = value
            else:
                self.data[key] = value

    def finalize(self) -> dict[Hashable, Any]:
        self.finalize_calls += 1
        return self.data.copy()

    def clear(self) -> None:
        self.clear_calls += 1
        self.data.clear()
        self.accumulate_calls.clear()


class FakeJobFactory(JobFactory):
    """Fake implementation of JobFactory for testing."""

    def __init__(self):
        self.created_jobs = []
        self.processors: dict[JobId, FakeProcessor] = {}

    def create(self, *, job_id: JobId, source_name: str, config: WorkflowConfig) -> Job:
        processor = FakeProcessor()
        self.processors[job_id] = processor

        # Simple source mapping for testing
        source_mapping = {source_name: "main_data", "aux_source": "aux_data"}

        job = Job(
            job_id=job_id,
            workflow_name=f"workflow_{config.identifier}",
            source_name=source_name,
            processor=processor,
            source_mapping=source_mapping,
        )

        self.created_jobs.append((job_id, source_name, config))
        return job


@pytest.fixture
def fake_processor():
    return FakeProcessor()


@pytest.fixture
def fake_job_factory():
    return FakeJobFactory()


@pytest.fixture
def sample_job(fake_processor):
    return Job(
        job_id=1,
        workflow_name="test_workflow",
        source_name="test_source",
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


class TestJob:
    def test_initial_state(self, sample_job):
        """Test initial state of a Job."""
        assert sample_job.start_time == -1
        assert sample_job.end_time == -1

    def test_add_data_sets_times(self, sample_job, sample_workflow_data):
        """Test that adding data sets start and end times."""
        sample_job.add(sample_workflow_data)

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

        sample_job.add(data1)
        assert sample_job.start_time == 100
        assert sample_job.end_time == 150

        sample_job.add(data2)
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

        sample_job.add(data)

        # Check that processor received only mapped data
        assert len(fake_processor.accumulate_calls) == 1
        accumulated = fake_processor.accumulate_calls[0]
        assert "main" in accumulated
        assert "aux" in accumulated
        assert accumulated["main"] == sc.scalar(42.0)
        assert accumulated["aux"] == sc.scalar(10.0)
        # unmapped_source should not appear
        assert len(accumulated) == 2  # Only main and aux should be present

    def test_get_returns_job_result(self, sample_job, sample_workflow_data):
        """Test that get() returns a proper JobResult."""
        sample_job.add(sample_workflow_data)
        result = sample_job.get()

        assert isinstance(result, JobResult)
        assert result.job_id == 1
        assert result.source_name == "test_source"
        assert result.name == "test_workflow"
        assert result.start_time == 100
        assert result.end_time == 200
        assert isinstance(result.data, sc.DataGroup)

    def test_get_calls_processor_finalize(self, sample_job, fake_processor):
        """Test that get() calls processor.finalize()."""
        sample_job.get()
        assert fake_processor.finalize_calls == 1

    def test_reset_clears_processor_and_times(
        self, sample_job, sample_workflow_data, fake_processor
    ):
        """Test that reset() clears processor and resets times."""
        sample_job.add(sample_workflow_data)
        assert sample_job.start_time == 100
        assert sample_job.end_time == 200

        sample_job.reset()

        assert fake_processor.clear_calls == 1
        assert sample_job.start_time == -1
        assert sample_job.end_time == -1


class TestJobManager:
    def test_initial_state(self, fake_job_factory):
        """Test initial state of JobManager."""
        manager = JobManager(fake_job_factory)

        assert manager.service_name == 'data_reduction'
        assert len(manager.active_jobs) == 0

    def test_schedule_job_creates_job(self, fake_job_factory):
        """Test that scheduling a job creates it using the factory."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        assert job_id == 0
        assert len(fake_job_factory.created_jobs) == 1
        assert fake_job_factory.created_jobs[0] == (0, "test_source", config)

    def test_schedule_multiple_jobs_increments_id(self, fake_job_factory):
        """Test that scheduling multiple jobs increments job IDs."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id1 = manager.schedule_job("source1", config)
        job_id2 = manager.schedule_job("source2", config)

        assert job_id1 == 0
        assert job_id2 == 1

    def test_push_data_activates_scheduled_jobs(self, fake_job_factory):
        """Test that pushing data activates jobs that should start."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)
        assert len(manager.active_jobs) == 0

        # Push data that should activate the job
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        assert len(manager.active_jobs) == 1

    def test_push_data_feeds_active_jobs(self, fake_job_factory):
        """Test that pushing data feeds all active jobs."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id1 = manager.schedule_job("source1", config)
        job_id2 = manager.schedule_job("source2", config)

        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="source1"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        # Both jobs should be active and receive data
        assert len(manager.active_jobs) == 2
        for job in manager.active_jobs:
            assert job.start_time == 100
            assert job.end_time == 200

    def test_stop_job_scheduled(self, fake_job_factory):
        """Test stopping a scheduled job removes it from scheduled jobs."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)
        assert job_id in manager._scheduled_jobs

        manager.stop_job(job_id)
        assert job_id not in manager._scheduled_jobs

    def test_stop_job_active_marks_for_finishing(self, fake_job_factory):
        """Test stopping an active job marks it for finishing."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Activate the job
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        assert job_id in manager._active_jobs
        manager.stop_job(job_id)

        assert job_id in manager._finishing_jobs
        assert job_id in manager._active_jobs  # Still active until compute_results

    def test_stop_job_nonexistent_raises_error(self, fake_job_factory):
        """Test stopping a non-existent job raises KeyError."""
        manager = JobManager(fake_job_factory)

        with pytest.raises(KeyError, match="Job 999 not found"):
            manager.stop_job(999)

    def test_reset_job_active(self, fake_job_factory):
        """Test resetting an active job calls its reset method."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Activate and feed data to the job
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        processor = fake_job_factory.processors[job_id]
        assert processor.clear_calls == 0

        manager.reset_job(job_id)
        assert processor.clear_calls == 1

    def test_reset_job_scheduled(self, fake_job_factory):
        """Test resetting a scheduled job calls its reset method."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)
        processor = fake_job_factory.processors[job_id]

        manager.reset_job(job_id)
        assert processor.clear_calls == 1

    def test_reset_job_nonexistent_raises_error(self, fake_job_factory):
        """Test resetting a non-existent job raises KeyError."""
        manager = JobManager(fake_job_factory)

        with pytest.raises(KeyError, match="Job 999 not found"):
            manager.reset_job(999)

    def test_compute_results_returns_job_results(self, fake_job_factory):
        """Test that compute_results returns results from all active jobs."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id1 = manager.schedule_job("source1", config)
        job_id2 = manager.schedule_job("source2", config)

        # Activate jobs
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="source1"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        results = manager.compute_results()

        assert len(results) == 2
        assert all(isinstance(result, JobResult) for result in results)

    def test_compute_results_removes_finishing_jobs(self, fake_job_factory):
        """Test that compute_results removes jobs marked for finishing."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Activate job
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        assert len(manager.active_jobs) == 1

        # Stop the job
        manager.stop_job(job_id)
        assert job_id in manager._finishing_jobs

        # Compute results should remove it
        manager.compute_results()
        assert len(manager.active_jobs) == 0
        assert len(manager._finishing_jobs) == 0

    def test_advance_to_time_activates_jobs_by_start_time(self, fake_job_factory):
        """Test that _advance_to_time activates jobs based on their start time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Manually set start time on the job (simulating a job that should start later)
        job = manager._scheduled_jobs[job_id]
        job._start_time = 150

        # Advance to time before job should start
        manager._advance_to_time(100, 200)
        assert len(manager.active_jobs) == 0

        # Advance to time when job should start
        manager._advance_to_time(150, 200)
        assert len(manager.active_jobs) == 1

    def test_advance_to_time_marks_jobs_for_finishing_by_end_time(
        self, fake_job_factory
    ):
        """Test that _advance_to_time marks jobs for finishing based on their end time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Activate job
        data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        assert len(manager._finishing_jobs) == 0

        # Advance to time after job should finish
        manager._advance_to_time(100, 200)
        assert job_id in manager._finishing_jobs
