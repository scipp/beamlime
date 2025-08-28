# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest
import sciline
from ess.reduce.streaming import StreamProcessor
from pydantic import BaseModel, ValidationError

from beamlime.config.workflow_spec import WorkflowConfig, WorkflowSpec
from beamlime.handlers.stream_processor_factory import StreamProcessorFactory


class MyParams(BaseModel):
    value: int = 42
    name: str = "test"


def make_dummy_stream_processor() -> StreamProcessor:
    """Fixture to create a mock StreamProcessor."""
    workflow = sciline.Pipeline()
    return StreamProcessor(
        base_workflow=workflow, dynamic_keys=(int,), target_keys=(), accumulators=()
    )


def make_dummy_stream_processor_with_source(*, source_name: str) -> StreamProcessor:
    """Fixture to create a mock StreamProcessor that uses source_name."""
    workflow = sciline.Pipeline()
    # In a real implementation, the source_name would be used to customize the processor
    return StreamProcessor(
        base_workflow=workflow, dynamic_keys=(int,), target_keys=(), accumulators=()
    )


def make_dummy_stream_processor_with_params(*, params: MyParams) -> StreamProcessor:
    """Fixture to create a mock StreamProcessor that uses params."""
    workflow = sciline.Pipeline()
    return StreamProcessor(
        base_workflow=workflow, dynamic_keys=(int,), target_keys=(), accumulators=()
    )


class TestStreamProcessorFactory:
    def test_init_factory_is_empty(self):
        factory = StreamProcessorFactory()
        assert len(factory) == 0
        assert list(factory) == []

    def test_register_adds_workflow_spec(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-workflow",
            version=1,
            title="Pretty name",
            description="Test description",
            params=None,
        )

        @factory.register(spec)
        def factory_func():
            return make_dummy_stream_processor()

        assert len(factory) == 1
        workflow_id = "test-instrument/test-namespace/test-workflow/1"
        assert workflow_id in factory
        stored_spec = factory[workflow_id]
        assert stored_spec.name == "test-workflow"
        assert stored_spec.description == "Test description"
        assert stored_spec.source_names == []

    def test_register_with_source_names(self):
        factory = StreamProcessorFactory()
        sources = ["source1", "source2"]
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-workflow",
            version=1,
            title="test-workflow",
            description="Test",
            source_names=sources,
            params=None,
        )

        @factory.register(spec)
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = "test-instrument/test-namespace/test-workflow/1"
        stored_spec = factory[workflow_id]
        assert stored_spec.source_names == sources

    def test_register_duplicate_id_raises_error(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-wf",
            version=1,
            title="test-workflow",
            description="Test",
            params=None,
        )

        @factory.register(spec)
        def factory_func():
            return make_dummy_stream_processor()

        with pytest.raises(
            ValueError,
            match="Workflow ID 'test-instrument/test-namespace/test-wf/1' is already "
            "registered.",
        ):
            factory.register(spec)(lambda: make_dummy_stream_processor())

    def test_create_returns_stream_processor(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-workflow",
            version=1,
            title="test-workflow",
            description="Test",
            params=None,
        )

        @factory.register(spec)
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = "test-instrument/test-namespace/test-workflow/1"
        config = WorkflowConfig(identifier=workflow_id)
        processor = factory.create(source_name="any-source", config=config)
        assert isinstance(processor, StreamProcessor)

    def test_create_with_source_name_parameter(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-workflow",
            version=1,
            title="test-workflow",
            description="Test",
            source_names=["source1"],
            params=None,
        )

        @factory.register(spec)
        def factory_func(*, source_name):
            return make_dummy_stream_processor_with_source(source_name=source_name)

        workflow_id = "test-instrument/test-namespace/test-workflow/1"
        config = WorkflowConfig(identifier=workflow_id)
        processor = factory.create(source_name="source1", config=config)
        assert isinstance(processor, StreamProcessor)

    def test_create_with_params(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-workflow",
            version=1,
            title="test-workflow",
            description="Test",
            params=None,
        )

        @factory.register(spec)
        def factory_func(*, params: MyParams):
            return make_dummy_stream_processor_with_params(params=params)

        workflow_id = "test-instrument/test-namespace/test-workflow/1"
        config = WorkflowConfig(
            identifier=workflow_id, params={"value": 100, "name": "custom"}
        )
        processor = factory.create(source_name="any-source", config=config)
        assert isinstance(processor, StreamProcessor)

    def test_create_invalid_params_raises_pydantic_error(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-workflow",
            version=1,
            title="test-workflow",
            description="Test",
            params=None,  # This will be auto-detected by the factory
        )

        @factory.register(spec)
        def factory_func(*, params: MyParams):
            return make_dummy_stream_processor_with_params(params=params)

        workflow_id = "test-instrument/test-namespace/test-workflow/1"
        config = WorkflowConfig(
            identifier=workflow_id,
            params={"value": "not-an-int", "name": "test"},  # Invalid type for 'value'
        )

        # This should raise a pydantic validation error
        with pytest.raises(ValidationError):
            factory.create(source_name="any-source", config=config)

    def test_unknown_workflow_id_raises_key_error(self):
        factory = StreamProcessorFactory()
        config = WorkflowConfig(identifier="non-existent-id")

        with pytest.raises(KeyError, match="Unknown workflow ID"):
            factory.create(source_name="any-source", config=config)

    def test_invalid_source_name_raises_value_error(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-workflow",
            version=1,
            title="test-workflow",
            description="Test",
            source_names=["allowed-source"],
            params=None,
        )

        @factory.register(spec)
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = "test-instrument/test-namespace/test-workflow/1"
        config = WorkflowConfig(identifier=workflow_id)

        with pytest.raises(ValueError, match="Source 'invalid-source' is not allowed"):
            factory.create(source_name="invalid-source", config=config)

    def test_multiple_registrations_create_distinct_entries(self):
        factory = StreamProcessorFactory()
        spec1 = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="workflow1",
            version=1,
            title="workflow1",
            description="Test 1",
            params=None,
        )
        spec2 = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="workflow2",
            version=1,
            title="workflow2",
            description="Test 2",
            params=None,
        )

        @factory.register(spec1)
        def factory_func1():
            return make_dummy_stream_processor()

        @factory.register(spec2)
        def factory_func2():
            return make_dummy_stream_processor()

        assert len(factory) == 2
        specs = list(factory.values())
        names = [spec.name for spec in specs]
        assert sorted(names) == ["workflow1", "workflow2"]

    def test_mapping_interface(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="workflow1",
            version=1,
            title="workflow1",
            description="Test",
            params=None,
        )

        @factory.register(spec)
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = "test-instrument/test-namespace/workflow1/1"
        assert factory[workflow_id].name == "workflow1"
        assert list(iter(factory)) == [workflow_id]
        assert len(factory) == 1
        assert list(factory.keys()) == [workflow_id]
        assert len(list(factory.values())) == 1
        assert next(iter(factory.items()))[0] == workflow_id

    def test_duplicate_workflow_names_different_versions(self):
        factory = StreamProcessorFactory()
        spec1 = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="same-name",
            version=1,
            title="V1",
            description="Test 1",
            params=None,
        )
        spec2 = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="same-name",
            version=2,
            title="V2",
            description="Test 2",
            params=None,
        )

        @factory.register(spec1)
        def factory_func1():
            return make_dummy_stream_processor()

        @factory.register(spec2)
        def factory_func2():
            return make_dummy_stream_processor()

        # Both functions should be registered with different IDs but same name
        assert len(factory) == 2
        specs = list(factory.values())
        names = [spec.name for spec in specs]
        assert names.count("same-name") == 2

        # IDs should be different
        workflow_ids = [
            "test-instrument/test-namespace/same-name/1",
            "test-instrument/test-namespace/same-name/2",
        ]
        assert all(wid in factory for wid in workflow_ids)

        # Both workflows should be callable
        config1 = WorkflowConfig(identifier=workflow_ids[0])
        config2 = WorkflowConfig(identifier=workflow_ids[1])
        processor1 = factory.create(source_name="any", config=config1)
        processor2 = factory.create(source_name="any", config=config2)
        assert isinstance(processor1, StreamProcessor)
        assert isinstance(processor2, StreamProcessor)

    def test_empty_name(self):
        factory = StreamProcessorFactory()
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="",
            version=1,
            title="",
            description="Test",
            params=None,
        )

        @factory.register(spec)
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = "test-instrument/test-namespace//1"
        stored_spec = factory[workflow_id]
        assert stored_spec.name == ""

        # Should still create a processor
        config = WorkflowConfig(identifier=workflow_id)
        processor = factory.create(source_name="any", config=config)
        assert isinstance(processor, StreamProcessor)

    def test_case_sensitivity_in_source_names(self):
        factory = StreamProcessorFactory()
        sources = ["Source1", "SOURCE2"]
        spec = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="test-workflow",
            version=1,
            title="test-workflow",
            description="Test",
            source_names=sources,
            params=None,
        )

        @factory.register(spec)
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = "test-instrument/test-namespace/test-workflow/1"
        config = WorkflowConfig(identifier=workflow_id)

        # These should work
        processor1 = factory.create(source_name="Source1", config=config)
        processor2 = factory.create(source_name="SOURCE2", config=config)
        assert isinstance(processor1, StreamProcessor)
        assert isinstance(processor2, StreamProcessor)

        # These should fail due to case sensitivity
        with pytest.raises(ValueError, match="is not allowed"):
            factory.create(source_name="source1", config=config)

        with pytest.raises(ValueError, match="is not allowed"):
            factory.create(source_name="source2", config=config)

    def test_source_names_property(self):
        factory = StreamProcessorFactory()
        spec1 = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="workflow1",
            version=1,
            title="workflow1",
            description="Test",
            source_names=["source1", "source2"],
            params=None,
        )
        spec2 = WorkflowSpec(
            instrument="test-instrument",
            namespace="test-namespace",
            name="workflow2",
            version=1,
            title="workflow2",
            description="Test",
            source_names=["source2", "source3"],
            params=None,
        )

        @factory.register(spec1)
        def factory_func1():
            return make_dummy_stream_processor()

        @factory.register(spec2)
        def factory_func2():
            return make_dummy_stream_processor()

        expected_sources = {"source1", "source2", "source3"}
        assert factory.source_names == expected_sources
