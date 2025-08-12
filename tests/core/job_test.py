# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from collections.abc import Hashable
from copy import deepcopy
from typing import Any

import pytest
import scipp as sc

from beamlime.config.workflow_spec import JobSchedule, WorkflowConfig
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
        assert schedule.start_time == 0
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
        config = WorkflowConfig(identifier="test_workflow")  # Start immediately

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

    def test_push_data_activates_scheduled_jobs_with_immediate_start(
        self, fake_job_factory
    ):
        """Test that pushing data activates jobs scheduled to start immediately."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")  # Start immediately

        _ = manager.schedule_job("test_source", config)
        assert len(manager.active_jobs) == 0

        # Push data that should activate the job (since start_time=-1)
        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        assert len(manager.active_jobs) == 1

    def test_push_data_activates_jobs_based_on_schedule(self, fake_job_factory):
        """Test that jobs are activated based on their scheduled start time."""
        manager = JobManager(fake_job_factory)
        config1 = WorkflowConfig(
            identifier="early_workflow", schedule=JobSchedule(start_time=50)
        )
        config2 = WorkflowConfig(
            identifier="late_workflow", schedule=JobSchedule(start_time=150)
        )

        _ = manager.schedule_job("source1", config1)
        _ = manager.schedule_job("source2", config2)
        assert len(manager.active_jobs) == 0

        # Push early data - should only activate job1
        early_data = WorkflowData(
            start_time=100,
            end_time=120,
            data={StreamId(name="source1"): sc.scalar(42.0)},
        )
        manager.push_data(early_data)
        assert len(manager.active_jobs) == 1

        # Push later data - should activate job2
        later_data = WorkflowData(
            start_time=160,
            end_time=180,
            data={StreamId(name="source2"): sc.scalar(42.0)},
        )
        manager.push_data(later_data)
        assert len(manager.active_jobs) == 2

    def test_push_data_feeds_active_jobs(self, fake_job_factory):
        """Test that pushing data feeds all active jobs."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(identifier="test_workflow")

        _ = manager.schedule_job("source1", config)
        _ = manager.schedule_job("source2", config)

        data = WorkflowData(
            start_time=100,
            end_time=200,
            data={StreamId(name="source1"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        # Both jobs should be active and receive data
        assert len(manager.active_jobs) == 2
        for job in manager.active_jobs:
            assert job.start_time == 100  # Data start time
            assert job.end_time == 200  # Data end time

    def test_stop_job_scheduled_removes_from_scheduled(self, fake_job_factory):
        """Test stopping a scheduled job removes it completely."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow", schedule=JobSchedule(start_time=200)
        )  # Start later

        job_id = manager.schedule_job("test_source", config)
        # Before data reaches start time, job should be scheduled but not active
        assert len(manager.active_jobs) == 0

        manager.stop_job(job_id)

        # After stopping, even when data reaches start time, job should not activate
        data = WorkflowData(
            start_time=250,
            end_time=300,
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
        config = WorkflowConfig(
            identifier="test_workflow", schedule=JobSchedule(start_time=200)
        )

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

        _ = manager.schedule_job("source1", config)
        _ = manager.schedule_job("source2", config)

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

    def test_job_lifecycle_with_schedule_based_activation(self, fake_job_factory):
        """Test complete job lifecycle with schedule-based activation."""
        manager = JobManager(fake_job_factory)
        config1 = WorkflowConfig(
            identifier="workflow1", schedule=JobSchedule(start_time=50, end_time=250)
        )
        config2 = WorkflowConfig(
            identifier="workflow2", schedule=JobSchedule(start_time=150, end_time=350)
        )

        # Schedule two jobs with different start times
        _ = manager.schedule_job("source1", config1)
        _ = manager.schedule_job("source2", config2)

        # Initially no active jobs
        assert len(manager.active_jobs) == 0

        # Push early data - should activate job1 only (start_time=50)
        early_data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="source1"): sc.scalar(10.0)},
        )
        manager.push_data(early_data)
        assert len(manager.active_jobs) == 1

        # Push later data - should activate job2 (start_time=150)
        later_data = WorkflowData(
            start_time=200,
            end_time=250,
            data={StreamId(name="source1"): sc.scalar(20.0)},
        )
        manager.push_data(later_data)
        assert len(manager.active_jobs) == 2

        # Push data that should finish job1 (end_time=250)
        finishing_data = WorkflowData(
            start_time=251,
            end_time=300,
            data={StreamId(name="source1"): sc.scalar(30.0)},
        )
        manager.push_data(finishing_data)

        # Get results - job1 should be finished and removed
        results = manager.compute_results()
        assert len(results) == 2  # Both jobs return results
        assert len(manager.active_jobs) == 1  # Only job2 remains

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

    def test_jobs_finish_based_on_schedule_end_time(self, fake_job_factory):
        """Test that jobs are marked for finishing based on schedule end_time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow",
            schedule=JobSchedule(end_time=175),
        )

        _ = manager.schedule_job("test_source", config)

        # Activate job with initial data
        initial_data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 1

        # Push data with start_time that goes beyond the job's scheduled end_time
        finishing_data = WorkflowData(
            start_time=180,  # Beyond job's scheduled end_time of 175
            end_time=200,
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )
        manager.push_data(finishing_data)

        # Job should be marked for finishing
        results = manager.compute_results()
        assert len(results) == 1
        assert len(manager.active_jobs) == 0  # Job should be finished and removed

    def test_multiple_jobs_different_schedule_end_times(self, fake_job_factory):
        """Test handling multiple jobs with different scheduled end times."""
        manager = JobManager(fake_job_factory)
        config1 = WorkflowConfig(
            identifier="workflow1", schedule=JobSchedule(end_time=150)
        )
        config2 = WorkflowConfig(
            identifier="workflow2", schedule=JobSchedule(end_time=200)
        )
        config3 = WorkflowConfig(
            identifier="workflow3", schedule=JobSchedule(end_time=300)
        )

        # Schedule three jobs with different end times
        _ = manager.schedule_job("source1", config1)
        _ = manager.schedule_job("source2", config2)
        _ = manager.schedule_job("source3", config3)

        # Activate all jobs
        initial_data = WorkflowData(
            start_time=100,
            end_time=120,
            data={
                StreamId(name="source1"): sc.scalar(10.0),
                StreamId(name="source2"): sc.scalar(20.0),
                StreamId(name="source3"): sc.scalar(30.0),
            },
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 3

        # Push data with start_time=175, should finish job1 (end_time=150), not others
        intermediate_data = WorkflowData(
            start_time=175,
            end_time=180,
            data={StreamId(name="source2"): sc.scalar(5.0)},
        )
        manager.push_data(intermediate_data)

        # Compute results should finish job1
        results = manager.compute_results()
        assert len(results) == 3  # All jobs return results
        assert len(manager.active_jobs) == 2  # Job1 should be removed

        # Push data with start_time=250, should finish job2 (end_time=200) but not job3
        later_data = WorkflowData(
            start_time=250,
            end_time=260,
            data={StreamId(name="source3"): sc.scalar(15.0)},
        )
        manager.push_data(later_data)

        # Compute results should finish job2
        results = manager.compute_results()
        assert len(results) == 2  # Both remaining jobs return results
        assert len(manager.active_jobs) == 1  # Only job3 should remain

    def test_job_finishing_edge_case_exact_schedule_end_time(self, fake_job_factory):
        """Test job finishing when data start_time is exactly scheduled end_time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow",
            schedule=JobSchedule(end_time=200),
        )

        _ = manager.schedule_job("test_source", config)

        # Activate job
        initial_data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 1

        # Push data with start_time exactly matching job's scheduled start_time
        exact_data = WorkflowData(
            start_time=200,  # Exactly matches job's end_time
            end_time=250,
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )
        manager.push_data(exact_data)

        # Job should be marked for finishing
        results = manager.compute_results()
        assert len(results) == 1
        assert len(manager.active_jobs) == 0  # Job should be finished

    def test_no_premature_job_finishing(self, fake_job_factory):
        """Test jobs don't finish prematurely when data is before scheduled end_time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow",
            schedule=JobSchedule(end_time=300),
        )

        _ = manager.schedule_job("test_source", config)

        # Activate job
        initial_data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 1

        # Push data with start_time < job's scheduled end_time
        early_data = WorkflowData(
            start_time=200,  # Before job's scheduled end_time of 300
            end_time=250,
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )
        manager.push_data(early_data)

        # Job should NOT be marked for finishing
        results = manager.compute_results()
        assert len(results) == 1
        assert len(manager.active_jobs) == 1  # Job should still be active

    def test_job_finishing_with_mixed_scenarios(self, fake_job_factory):
        """
        Test complex scenario with job stopping, schedule-based finishing,
        and continuation.
        """
        manager = JobManager(fake_job_factory)
        config1 = WorkflowConfig(
            identifier="workflow1", schedule=JobSchedule(end_time=160)
        )
        config2 = WorkflowConfig(
            identifier="workflow2", schedule=JobSchedule(end_time=250)
        )
        config3 = WorkflowConfig(
            identifier="workflow3", schedule=JobSchedule(end_time=None)
        )  # No end time

        # Schedule three jobs
        _ = manager.schedule_job("source1", config1)
        job_id2 = manager.schedule_job("source2", config2)
        _ = manager.schedule_job("source3", config3)

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

        # Push data that would finish job1 due to schedule end_time, keep job3 active
        finishing_data = WorkflowData(
            start_time=170,  # Beyond job1's scheduled end_time of 160
            end_time=180,
            data={
                StreamId(name="source1"): sc.scalar(5.0),
                StreamId(name="source3"): sc.scalar(15.0),
            },
        )
        manager.push_data(finishing_data)

        # Compute results should:
        # - Return results from all 3 jobs
        # - Remove job1 (finished by schedule) and job2 (manually stopped)
        # - Keep job3 active (no end time)
        results = manager.compute_results()
        assert len(results) == 3
        assert len(manager.active_jobs) == 1  # Only job3 should remain

    def test_jobs_without_end_time_never_finish_automatically(self, fake_job_factory):
        """Test that jobs without scheduled end_time never finish automatically."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow",
            schedule=JobSchedule(end_time=None),
        )

        _ = manager.schedule_job("test_source", config)

        # Activate job and push lots of data
        for i in range(5):
            data = WorkflowData(
                start_time=100 + i * 100,
                end_time=150 + i * 100,
                data={StreamId(name="test_source"): sc.scalar(10.0 * i)},
            )
            manager.push_data(data)

        # Job should never be marked for finishing
        results = manager.compute_results()
        assert len(results) == 1
        assert len(manager.active_jobs) == 1  # Job should still be active

    def test_schedule_start_time_edge_cases(self, fake_job_factory):
        """Test edge cases for schedule start times."""
        manager = JobManager(fake_job_factory)

        # Test immediate start (-1)
        config_immediate = WorkflowConfig(identifier="immediate")
        # Test future start
        config_future = WorkflowConfig(
            identifier="future", schedule=JobSchedule(start_time=200)
        )
        # Test past start (should activate immediately when data arrives)
        config_past = WorkflowConfig(
            identifier="past", schedule=JobSchedule(start_time=50)
        )

        _ = manager.schedule_job("source1", config_immediate)
        _ = manager.schedule_job("source2", config_future)
        _ = manager.schedule_job("source3", config_past)

        # Push data at time 100
        data = WorkflowData(
            start_time=100,
            end_time=150,
            data={
                StreamId(name="source1"): sc.scalar(10.0),
                StreamId(name="source2"): sc.scalar(20.0),
                StreamId(name="source3"): sc.scalar(30.0),
            },
        )
        manager.push_data(data)

        # Should activate immediate and past, but not future
        assert len(manager.active_jobs) == 2

        # Push later data that should activate future job
        later_data = WorkflowData(
            start_time=250,
            end_time=300,
            data={StreamId(name="source2"): sc.scalar(40.0)},
        )
        manager.push_data(later_data)

        # All jobs should now be active
        assert len(manager.active_jobs) == 3

    def test_schedule_job_with_immediate_start_and_end_time_allowed(
        self, fake_job_factory
    ):
        """Test that immediate start (-1) with any end_time is allowed."""
        manager = JobManager(fake_job_factory)

        # This should be allowed: immediate start with specific end time
        config_valid = WorkflowConfig(
            identifier="valid_workflow",
            schedule=JobSchedule(end_time=100),
        )
        job_id = manager.schedule_job("test_source", config_valid)
        assert job_id == 0

    def test_job_with_zero_duration_after_immediate_start(self, fake_job_factory):
        """Test behavior of job with immediate start and very early end time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow", schedule=JobSchedule(end_time=50)
        )

        _ = manager.schedule_job("test_source", config)

        data = WorkflowData(
            start_time=30,  # Before job's end_time
            end_time=100,  # Beyond job's end_time of 50 - so job should finish
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        # Job should activate and immediately finish
        assert len(manager.active_jobs) == 1  # Activated

        results = manager.compute_results()
        assert len(results) == 1  # Returns result
        assert len(manager.active_jobs) == 0  # Finished and removed

    def test_job_schedule_edge_case_start_equals_data_time(self, fake_job_factory):
        """
        Test job activation when data start_time exactly matches scheduled start_time.
        """
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow",
            schedule=JobSchedule(start_time=100, end_time=200),
        )

        _ = manager.schedule_job("test_source", config)

        # Push data with start_time exactly matching job's scheduled start_time
        data = WorkflowData(
            start_time=100,  # Exactly matches job's start_time
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)

        # Job should be activated
        assert len(manager.active_jobs) == 1

    def test_job_schedule_edge_case_end_equals_data_time(self, fake_job_factory):
        """Test job finishing when data end_time exactly matches scheduled end_time."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow",
            schedule=JobSchedule(end_time=200),
        )

        _ = manager.schedule_job("test_source", config)

        # Activate job
        initial_data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(10.0)},
        )
        manager.push_data(initial_data)
        assert len(manager.active_jobs) == 1

        # Push data with end_time exactly matching job's scheduled end_time
        final_data = WorkflowData(
            start_time=180,
            end_time=200,  # Exactly matches job's end_time
            data={StreamId(name="test_source"): sc.scalar(20.0)},
        )
        manager.push_data(final_data)

        # Job should NOT be marked for finishing yet (end_time <= end_time, not <)
        assert len(manager.active_jobs) == 1

        # But the next data batch should finish it
        beyond_data = WorkflowData(
            start_time=201,  # Beyond job's end_time
            end_time=250,
            data={StreamId(name="test_source"): sc.scalar(5.0)},
        )
        manager.push_data(beyond_data)

        results = manager.compute_results()
        assert len(results) == 1
        assert len(manager.active_jobs) == 0  # Job should be finished

    def test_multiple_jobs_same_schedule_times(self, fake_job_factory):
        """Test multiple jobs with identical start and end times."""
        manager = JobManager(fake_job_factory)
        config = WorkflowConfig(
            identifier="test_workflow",
            schedule=JobSchedule(start_time=100, end_time=200),
        )

        # Schedule multiple jobs with same timing
        _ = manager.schedule_job("source1", config)
        _ = manager.schedule_job("source2", config)
        _ = manager.schedule_job("source3", config)

        # All should activate together
        data = WorkflowData(
            start_time=150,
            end_time=170,
            data={
                StreamId(name="source1"): sc.scalar(10.0),
                StreamId(name="source2"): sc.scalar(20.0),
                StreamId(name="source3"): sc.scalar(30.0),
            },
        )
        manager.push_data(data)
        assert len(manager.active_jobs) == 3

        # All should finish together
        finishing_data = WorkflowData(
            start_time=250,  # Beyond all jobs' end_time
            end_time=270,
            data={StreamId(name="source1"): sc.scalar(5.0)},
        )
        manager.push_data(finishing_data)

        results = manager.compute_results()
        assert len(results) == 3
        assert len(manager.active_jobs) == 0

    def test_negative_start_times_other_than_minus_one(self, fake_job_factory):
        """Test behavior with negative start times other than -1."""
        manager = JobManager(fake_job_factory)

        # Negative start times other than -1 should be treated as regular timestamps
        config = WorkflowConfig(
            identifier="test_workflow",
            schedule=JobSchedule(start_time=-100, end_time=200),
        )
        _ = manager.schedule_job("test_source", config)

        # Data with positive time should activate job (since -100 < any positive time)
        data = WorkflowData(
            start_time=100,
            end_time=150,
            data={StreamId(name="test_source"): sc.scalar(42.0)},
        )
        manager.push_data(data)
        assert len(manager.active_jobs) == 1
