# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import uuid

import pytest
import scipp as sc

from beamlime.config.workflow_spec import JobId, ResultKey, WorkflowId
from beamlime.core.message import Message, StreamId, StreamKind
from beamlime.dashboard.data_service import DataService
from beamlime.dashboard.orchestrator import Orchestrator


def make_job_number() -> uuid.UUID:
    """Generate a random UUID for job number."""
    return uuid.uuid4()


class FakeMessageSource:
    """Simple fake message source for testing."""

    def __init__(self):
        self._messages = []

    def add_message(self, stream_name: str, data: sc.DataArray, timestamp: int = 1000):
        """Add a message to be returned by get_messages."""
        message = Message(
            timestamp=timestamp,
            stream=StreamId(kind=StreamKind.BEAMLIME_DATA, name=stream_name),
            value=data,
        )
        self._messages.append(message)

    def get_messages(self):
        """Return all messages and clear the internal list."""
        messages = self._messages.copy()
        self._messages.clear()
        return messages


class TestOrchestrator:
    def test_update_with_no_messages(self) -> None:
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        orchestrator.update()

        assert len(data_service) == 0

    def test_update_with_single_message(self) -> None:
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        workflow_id = WorkflowId(
            instrument="test_instrument",
            namespace="test_namespace",
            name="test_workflow",
            version=1,
        )
        job_id = JobId(source_name="detector1", job_number=make_job_number())
        result_key = ResultKey(workflow_id=workflow_id, job_id=job_id)

        data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))
        source.add_message(result_key.model_dump_json(), data)

        orchestrator.update()

        assert result_key in data_service
        assert sc.identical(data_service[result_key], data)

    def test_update_with_multiple_messages(self) -> None:
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        workflow_id1 = WorkflowId(
            instrument="test_instrument",
            namespace="test_namespace",
            name="workflow1",
            version=1,
        )
        workflow_id2 = WorkflowId(
            instrument="test_instrument",
            namespace="test_namespace",
            name="workflow2",
            version=1,
        )

        job_id1 = JobId(source_name="detector1", job_number=make_job_number())
        job_id2 = JobId(source_name="detector2", job_number=make_job_number())

        result_key1 = ResultKey(workflow_id=workflow_id1, job_id=job_id1)
        result_key2 = ResultKey(workflow_id=workflow_id2, job_id=job_id2)

        data1 = sc.DataArray(sc.array(dims=['x'], values=[1, 2]))
        data2 = sc.DataArray(sc.array(dims=['y'], values=[3, 4, 5]))

        source.add_message(result_key1.model_dump_json(), data1)
        source.add_message(result_key2.model_dump_json(), data2)

        orchestrator.update()

        assert result_key1 in data_service
        assert result_key2 in data_service
        assert sc.identical(data_service[result_key1], data1)
        assert sc.identical(data_service[result_key2], data2)

    def test_update_overwrites_existing_data(self) -> None:
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        workflow_id = WorkflowId(
            instrument="test_instrument",
            namespace="test_namespace",
            name="test_workflow",
            version=1,
        )
        job_id = JobId(source_name="detector1", job_number=make_job_number())
        result_key = ResultKey(workflow_id=workflow_id, job_id=job_id)

        original_data = sc.DataArray(sc.array(dims=['x'], values=[1, 2]))
        new_data = sc.DataArray(sc.array(dims=['y'], values=[3, 4, 5]))

        # Add initial data
        source.add_message(result_key.model_dump_json(), original_data)
        orchestrator.update()
        assert sc.identical(data_service[result_key], original_data)

        # Overwrite with new data
        source.add_message(result_key.model_dump_json(), new_data)
        orchestrator.update()
        assert sc.identical(data_service[result_key], new_data)

    def test_update_with_output_name(self) -> None:
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        workflow_id = WorkflowId(
            instrument="test_instrument",
            namespace="test_namespace",
            name="test_workflow",
            version=1,
        )
        job_id = JobId(source_name="detector1", job_number=make_job_number())
        result_key = ResultKey(
            workflow_id=workflow_id, job_id=job_id, output_name="processed_data"
        )

        data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))
        source.add_message(result_key.model_dump_json(), data)

        orchestrator.update()

        assert result_key in data_service
        assert sc.identical(data_service[result_key], data)

    def test_forward_with_valid_result_key(self) -> None:
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        workflow_id = WorkflowId(
            instrument="test_instrument",
            namespace="test_namespace",
            name="test_workflow",
            version=1,
        )
        job_id = JobId(source_name="detector1", job_number=make_job_number())
        result_key = ResultKey(workflow_id=workflow_id, job_id=job_id)

        data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))

        orchestrator.forward(result_key.model_dump_json(), data)

        assert result_key in data_service
        assert sc.identical(data_service[result_key], data)

    def test_forward_with_invalid_json(self) -> None:
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))

        # JSON parsing or Pydantic validation error
        with pytest.raises(ValueError, match="Invalid JSON"):
            orchestrator.forward("invalid_json", data)

    def test_forward_with_different_data_types(self) -> None:
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        workflow_id = WorkflowId(
            instrument="test_instrument",
            namespace="test_namespace",
            name="test_workflow",
            version=1,
        )

        job_id1 = JobId(source_name="detector1", job_number=make_job_number())
        job_id2 = JobId(source_name="detector1", job_number=make_job_number())
        job_id3 = JobId(source_name="detector1", job_number=make_job_number())

        result_key1 = ResultKey(
            workflow_id=workflow_id, job_id=job_id1, output_name="int_data"
        )
        result_key2 = ResultKey(
            workflow_id=workflow_id, job_id=job_id2, output_name="float_data"
        )
        result_key3 = ResultKey(
            workflow_id=workflow_id, job_id=job_id3, output_name="string_data"
        )

        int_data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))
        float_data = sc.DataArray(sc.array(dims=['y'], values=[1.5, 2.5]))
        string_data = sc.DataArray(sc.array(dims=['z'], values=['a', 'b']))

        orchestrator.forward(result_key1.model_dump_json(), int_data)
        orchestrator.forward(result_key2.model_dump_json(), float_data)
        orchestrator.forward(result_key3.model_dump_json(), string_data)

        assert sc.identical(data_service[result_key1], int_data)
        assert sc.identical(data_service[result_key2], float_data)
        assert sc.identical(data_service[result_key3], string_data)

    def test_transaction_mechanism(self) -> None:
        """Test that updates are batched in transactions."""
        source = FakeMessageSource()
        data_service = DataService()
        orchestrator = Orchestrator(source, data_service)

        # Track transaction calls
        transaction_started = False
        original_transaction = data_service.transaction

        def mock_transaction():
            nonlocal transaction_started
            transaction_started = True
            return original_transaction()

        data_service.transaction = mock_transaction

        workflow_id = WorkflowId(
            instrument="test_instrument",
            namespace="test_namespace",
            name="test_workflow",
            version=1,
        )
        job_id = JobId(source_name="detector1", job_number=make_job_number())
        result_key = ResultKey(workflow_id=workflow_id, job_id=job_id)

        data = sc.DataArray(sc.array(dims=['x'], values=[1, 2, 3]))
        source.add_message(result_key.model_dump_json(), data)

        orchestrator.update()

        assert transaction_started
        assert result_key in data_service
