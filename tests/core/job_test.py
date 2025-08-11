# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from collections.abc import Hashable
from copy import deepcopy
from typing import Any

import pytest
import scipp as sc

from beamlime.config.workflow_spec import WorkflowConfig
from beamlime.core.job import (
    Job,
    JobFactory,
    JobId,
    JobManager,
    JobResult,
    WorkflowData,
)
from beamlime.core.message import StreamId


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
                self.data[key] += value
            else:
                self.data[key] = deepcopy(value)

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

    def test_stop_job_scheduled_removes_from_scheduled(self, fake_job_factory):
        """Test stopping a scheduled job removes it completely."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)
        # Before push_data, job should be scheduled but not active
        assert len(manager.active_jobs) == 0

        manager.stop_job(job_id)

        # After stopping, push_data should not activate the job
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)
        assert len(manager.active_jobs) == 0

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
        assert len(manager.active_jobs) == 1

        manager.stop_job(job_id)
        # Job should still be active until compute_results is called
        assert len(manager.active_jobs) == 1

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

        # Verify job has data
        assert manager.active_jobs[0].start_time == 100
        assert manager.active_jobs[0].end_time == 200

        manager.reset_job(job_id)

        # Verify job was reset
        assert manager.active_jobs[0].start_time == -1
        assert manager.active_jobs[0].end_time == -1
        assert fake_job_factory.processors[job_id].clear_calls == 1

    def test_reset_job_scheduled(self, fake_job_factory):
        """Test resetting a scheduled job calls its reset method."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        manager.reset_job(job_id)
        assert fake_job_factory.processors[job_id].clear_calls == 1

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

    def test_compute_results_removes_stopped_jobs(self, fake_job_factory):
        """Test that compute_results removes jobs that were stopped."""
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
        assert len(manager.active_jobs) == 1  # Still active

        # Compute results should remove it
        results = manager.compute_results()
        assert len(results) == 1  # Should still return result
        assert len(manager.active_jobs) == 0  # Should be removed now

    def test_job_lifecycle_with_time_based_activation(self, fake_job_factory):
        """Test complete job lifecycle with time-based activation."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        # Schedule two jobs
        job_id1 = manager.schedule_job("source1", config)
        job_id2 = manager.schedule_job("source2", config)

        # Initially no active jobs
        assert len(manager.active_jobs) == 0

        # Push early data - should activate both jobs
        early_data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="source1"): sc.scalar(10.0)},
        )
        manager.push_data(early_data)
        assert len(manager.active_jobs) == 2

        # Push later data - should continue feeding jobs
        later_data = WorkflowData(
            start_time=151,
            end_time=200,
            data={StreamId(name="source1"): sc.scalar(20.0)},
        )
        manager.push_data(later_data)

        # Verify jobs received both data pushes
        for job in manager.active_jobs:
            assert job.start_time == 100
            assert job.end_time == 200

        # Get results
        results = manager.compute_results()
        assert len(results) == 2

    def test_multiple_data_accumulation(self, fake_job_factory):
        """Test that multiple data pushes accumulate correctly in jobs."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Push multiple data batches
        data1 = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        data2 = WorkflowData(
            start_time=151,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )

        manager.push_data(data1)
        manager.push_data(data2)

        # Verify processor received both accumulate calls
        processor = fake_job_factory.processors[job_id]
        assert len(processor.accumulate_calls) == 2
        assert sc.identical(processor.accumulate_calls[0]["main_data"], sc.scalar(10.0))
        assert sc.identical(processor.accumulate_calls[1]["main_data"], sc.scalar(20.0))

        # Verify accumulated data (our fake processor sums numeric values)
        results = manager.compute_results()
        assert len(results) == 1
        # The accumulated value should be 30.0 (10.0 + 20.0)
        assert processor.data["main_data"] == 30.0

    def test_jobs_finish_based_on_end_time(self, fake_job_factory):
        """Test that jobs are marked for finishing when data end_time exceeds job end_time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Activate job with initial data
        initial_data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 1
        assert manager.active_jobs[0].end_time == 150

        # Push data that goes beyond the job's current end_time
        # This should mark the job for finishing
        finishing_data = WorkflowData(
            start_time=151,
            end_time=200,  # Beyond job's current end_time of 150
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )
        manager.push_data(finishing_data)

        # Job should still be active until compute_results is called
        assert len(manager.active_jobs) == 1
        assert manager.active_jobs[0].end_time == 200  # Updated to new end_time

        # After compute_results, job should be removed
        results = manager.compute_results()
        assert len(results) == 1
        assert len(manager.active_jobs) == 0  # Job should be finished and removed

    def test_multiple_jobs_different_end_times(self, fake_job_factory):
        """Test handling multiple jobs with different end times."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        # Schedule three jobs
        job_id1 = manager.schedule_job("source1", config)
        job_id2 = manager.schedule_job("source2", config)
        job_id3 = manager.schedule_job("source3", config)

        # Activate all jobs with different end times
        data1 = WorkflowData(
            start_time=100,
            end_time=150,  # Job 1 ends at 150
            data={StreamId(name="source1"): sc.scalar(10.0)},
        )
        data2 = WorkflowData(
            start_time=100,
            end_time=200,  # Job 2 ends at 200
            data={StreamId(name="source2"): sc.scalar(20.0)},
        )
        data3 = WorkflowData(
            start_time=100,
            end_time=300,  # Job 3 ends at 300
            data={StreamId(name="source3"): sc.scalar(30.0)},
        )

        manager.push_data(data1)
        manager.push_data(data2)
        manager.push_data(data3)
        assert len(manager.active_jobs) == 3

        # Push data with end_time=175, should finish job1 (end_time=150) but not others
        intermediate_data = WorkflowData(
            start_time=151,
            end_time=175,
            data={StreamId(name="source1"): sc.scalar(5.0)},
        )
        manager.push_data(intermediate_data)

        # All jobs still active until compute_results
        assert len(manager.active_jobs) == 3

        # Compute results should finish job1
        results = manager.compute_results()
        assert len(results) == 3  # All jobs return results
        assert len(manager.active_jobs) == 2  # Job1 should be removed

        # Push data with end_time=250, should finish job2 (end_time=200) but not job3
        later_data = WorkflowData(
            start_time=201,
            end_time=250,
            data={StreamId(name="source3"): sc.scalar(15.0)},
        )
        manager.push_data(later_data)

        # Compute results should finish job2
        results = manager.compute_results()
        assert len(results) == 2  # Both remaining jobs return results
        assert len(manager.active_jobs) == 1  # Only job3 should remain

    def test_job_finishing_edge_case_exact_end_time(self, fake_job_factory):
        """Test job finishing when data end_time exactly matches job end_time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Activate job
        initial_data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 1

        # Push data with end_time exactly matching job's end_time
        exact_data = WorkflowData(
            start_time=151,
            end_time=200,  # Exactly matches job's end_time
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )
        manager.push_data(exact_data)

        # Job should be marked for finishing
        results = manager.compute_results()
        assert len(results) == 1
        assert len(manager.active_jobs) == 0  # Job should be finished

    def test_no_premature_job_finishing(self, fake_job_factory):
        """Test that jobs don't finish prematurely when data end_time is before job end_time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        job_id = manager.schedule_job("test_source", config)

        # Activate job with end_time=200
        initial_data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 1

        # Push data with end_time < job's end_time
        early_data = WorkflowData(
            start_time=151,
            end_time=180,  # Before job's end_time of 200
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )
        manager.push_data(early_data)

        # Job should NOT be marked for finishing
        results = manager.compute_results()
        assert len(results) == 1
        assert len(manager.active_jobs) == 1  # Job should still be active

    def test_job_finishing_with_mixed_scenarios(self, fake_job_factory):
        """Test complex scenario with job stopping, finishing, and continuation."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        # Schedule three jobs
        job_id1 = manager.schedule_job("source1", config)
        job_id2 = manager.schedule_job("source2", config)
        job_id3 = manager.schedule_job("source3", config)

        # Activate all jobs
        initial_data = WorkflowData(
            start_time=100,
            end_time=150,
            data={
                StreamId(name="source1"): sc.scalar(10.0),
                StreamId(name="source2"): sc.scalar(20.0),
                StreamId(name="source3"): sc.scalar(30.0),
            },
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 3

        # Manually stop job2
        manager.stop_job(job_id2)

        # Push data that would finish job1 due to end_time, keep job3 active
        finishing_data = WorkflowData(
            start_time=151,
            end_time=160,  # Beyond job1's end_time of 150, but job3 should continue
            data={
                StreamId(name="source1"): sc.scalar(5.0),
                StreamId(name="source3"): sc.scalar(15.0),
            },
        )
        manager.push_data(finishing_data)

        # Compute results should:
        # - Return results from all 3 jobs
        # - Remove job1 (finished by time) and job2 (manually stopped)
        # - Keep job3 active
        results = manager.compute_results()
        assert len(results) == 3
        assert len(manager.active_jobs) == 1  # Only job3 should remain
