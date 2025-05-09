# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest
import sciline
from ess.reduce.streaming import StreamProcessor

from beamlime.handlers.stream_processor_factory import StreamProcessorFactory


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


class TestStreamProcessorFactory:
    def test_init_factory_is_empty(self):
        factory = StreamProcessorFactory()
        assert len(factory) == 0
        assert list(factory) == []

    def test_register_adds_workflow_spec(self):
        factory = StreamProcessorFactory()

        @factory.register(name="test-workflow", description="Test description")
        def factory_func():
            return make_dummy_stream_processor()

        assert len(factory) == 1
        workflow_id = next(iter(factory))
        spec = factory[workflow_id]
        assert spec.name == "test-workflow"
        assert spec.description == "Test description"
        assert spec.source_names == []
        assert spec.parameters == []

    def test_register_with_source_names(self):
        factory = StreamProcessorFactory()
        sources = ["source1", "source2"]

        @factory.register(name="test-workflow", source_names=sources)
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = next(iter(factory))
        spec = factory[workflow_id]
        assert spec.source_names == sources

    def test_create_returns_stream_processor(self):
        factory = StreamProcessorFactory()

        @factory.register(name="test-workflow")
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = next(iter(factory))
        processor = factory.create(workflow_id=workflow_id, source_name="any-source")
        assert isinstance(processor, StreamProcessor)

    def test_create_with_source_name_parameter(self):
        factory = StreamProcessorFactory()

        @factory.register(name="test-workflow", source_names=["source1"])
        def factory_func(*, source_name):
            return make_dummy_stream_processor_with_source(source_name=source_name)

        workflow_id = next(iter(factory))
        processor = factory.create(workflow_id=workflow_id, source_name="source1")
        assert isinstance(processor, StreamProcessor)

    def test_unknown_workflow_id_raises_key_error(self):
        factory = StreamProcessorFactory()
        with pytest.raises(KeyError, match="Unknown workflow ID"):
            factory.create(workflow_id="non-existent-id", source_name="any-source")

    def test_invalid_source_name_raises_value_error(self):
        factory = StreamProcessorFactory()

        @factory.register(name="test-workflow", source_names=["allowed-source"])
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = next(iter(factory))
        with pytest.raises(ValueError, match="Source 'invalid-source' is not allowed"):
            factory.create(workflow_id=workflow_id, source_name="invalid-source")

    def test_multiple_registrations_create_distinct_entries(self):
        factory = StreamProcessorFactory()

        @factory.register(name="workflow1")
        def factory_func1():
            return make_dummy_stream_processor()

        @factory.register(name="workflow2")
        def factory_func2():
            return make_dummy_stream_processor()

        assert len(factory) == 2
        specs = list(factory.values())
        names = [spec.name for spec in specs]
        assert sorted(names) == ["workflow1", "workflow2"]

    def test_mapping_interface(self):
        factory = StreamProcessorFactory()

        @factory.register(name="workflow1")
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = next(iter(factory))

        assert factory[workflow_id].name == "workflow1"
        assert list(iter(factory)) == [workflow_id]
        assert len(factory) == 1
        assert list(factory.keys()) == [workflow_id]
        assert len(list(factory.values())) == 1
        assert next(iter(factory.items()))[0] == workflow_id

    def test_duplicate_workflow_names(self):
        factory = StreamProcessorFactory()

        @factory.register(name="same-name")
        def factory_func1():
            return make_dummy_stream_processor()

        @factory.register(name="same-name")
        def factory_func2():
            return make_dummy_stream_processor()

        # Both functions should be registered with different IDs but same name
        assert len(factory) == 2
        specs = list(factory.values())
        names = [spec.name for spec in specs]
        assert names.count("same-name") == 2

        # IDs should be different
        workflow_ids = list(factory.keys())
        assert workflow_ids[0] != workflow_ids[1]

        # Both workflows should be callable
        processor1 = factory.create(workflow_id=workflow_ids[0], source_name="any")
        processor2 = factory.create(workflow_id=workflow_ids[1], source_name="any")
        assert isinstance(processor1, StreamProcessor)
        assert isinstance(processor2, StreamProcessor)

    def test_empty_name(self):
        factory = StreamProcessorFactory()

        @factory.register(name="")
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = next(iter(factory))
        spec = factory[workflow_id]
        assert spec.name == ""

        # Should still create a processor
        processor = factory.create(workflow_id=workflow_id, source_name="any")
        assert isinstance(processor, StreamProcessor)

    def test_case_sensitivity_in_source_names(self):
        factory = StreamProcessorFactory()
        sources = ["Source1", "SOURCE2"]

        @factory.register(name="test-workflow", source_names=sources)
        def factory_func():
            return make_dummy_stream_processor()

        workflow_id = next(iter(factory))

        # These should work
        processor1 = factory.create(workflow_id=workflow_id, source_name="Source1")
        processor2 = factory.create(workflow_id=workflow_id, source_name="SOURCE2")
        assert isinstance(processor1, StreamProcessor)
        assert isinstance(processor2, StreamProcessor)

        # These should fail due to case sensitivity
        with pytest.raises(ValueError, match="is not allowed"):
            factory.create(workflow_id=workflow_id, source_name="source1")

        with pytest.raises(ValueError, match="is not allowed"):
            factory.create(workflow_id=workflow_id, source_name="source2")

    def test_lookup_workflow_by_id(self):
        factory = StreamProcessorFactory()

        @factory.register(name="workflow-a")
        def factory_func_a():
            return make_dummy_stream_processor()

        @factory.register(name="workflow-b")
        def factory_func_b():
            return make_dummy_stream_processor()

        workflow_ids = list(factory.keys())

        # Get the ID for a specific workflow
        workflow_a_id = next(
            id_ for id_ in workflow_ids if factory[id_].name == "workflow-a"
        )
        workflow_b_id = next(
            id_ for id_ in workflow_ids if factory[id_].name == "workflow-b"
        )

        # Verify we can create processors with specific IDs
        processor_a = factory.create(workflow_id=workflow_a_id, source_name="any")
        processor_b = factory.create(workflow_id=workflow_b_id, source_name="any")

        assert isinstance(processor_a, StreamProcessor)
        assert isinstance(processor_b, StreamProcessor)
