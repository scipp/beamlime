# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.config.keys import WORKFLOW_CONFIG, WORKFLOW_STATUS
from beamlime.config.models import ConfigKey
from beamlime.config.schema_registry import get_schema_registry
from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowStatus,
    WorkflowStatusType,
)
from beamlime.dashboard.config_service import ConfigService
from beamlime.dashboard.message_bridge import FakeMessageBridge
from beamlime.dashboard.schema_validator import PydanticSchemaValidator
from beamlime.dashboard.workflow_config_service import (
    ConfigServiceAdapter,
    WorkflowConfigService,
)


@pytest.fixture
def fake_message_bridge():
    """Create a fake message bridge for testing."""
    return FakeMessageBridge[ConfigKey, dict]()


@pytest.fixture
def config_service(fake_message_bridge):
    """Create a ConfigService with fake message bridge."""
    schema_validator = PydanticSchemaValidator(get_schema_registry())
    return ConfigService(schema_validator, fake_message_bridge)


@pytest.fixture
def workflow_config_service(config_service) -> WorkflowConfigService:
    """Create a ConfigServiceAdapter for testing."""
    source_names = ["source1", "source2"]
    return ConfigServiceAdapter(config_service, source_names)


@pytest.fixture
def sample_workflow_config():
    """Create a sample workflow config for testing."""
    return WorkflowConfig(identifier="test_workflow", params={"param1": "value1"})


@pytest.fixture
def sample_workflow_status():
    """Create a sample workflow status for testing."""
    return WorkflowStatus(
        source_name="source1",
        workflow_id="test_workflow",
        status=WorkflowStatusType.RUNNING,
        message="Running successfully",
    )


@pytest.fixture
def sample_persistent_configs():
    """Create sample persistent configs for testing."""
    persistent_config = PersistentWorkflowConfig(
        source_names=["source1"],
        config=WorkflowConfig(identifier="saved_workflow", values={"param": "value"}),
    )
    return PersistentWorkflowConfigs(configs={"saved_workflow": persistent_config})


def test_get_persistent_configs_default(workflow_config_service):
    """Test getting persistent configs returns default when none exist."""
    configs = workflow_config_service.get_persistent_configs()

    assert isinstance(configs, PersistentWorkflowConfigs)
    assert configs.configs == {}


def test_save_and_get_persistent_configs(
    workflow_config_service, sample_persistent_configs, fake_message_bridge
):
    """Test saving and retrieving persistent configs."""
    workflow_config_service.save_persistent_configs(sample_persistent_configs)

    # Process the message to simulate round-trip through message bridge
    fake_message_bridge.add_incoming_message(
        fake_message_bridge.get_published_messages()[0]
    )
    workflow_config_service._config_service.process_incoming_messages()

    retrieved_configs = workflow_config_service.get_persistent_configs()
    assert retrieved_configs.configs == sample_persistent_configs.configs


def test_send_workflow_config(
    workflow_config_service, sample_workflow_config, fake_message_bridge
):
    """Test sending workflow config to a source."""
    source_name = "source1"
    workflow_config_service.send_workflow_config(source_name, sample_workflow_config)

    # Check that message was published
    published_messages = fake_message_bridge.get_published_messages()
    assert len(published_messages) == 1

    key, value = published_messages[0]
    assert key.source_name == source_name
    assert key.service_name == WORKFLOW_CONFIG.service_name
    assert key.key == WORKFLOW_CONFIG.key
    assert value["identifier"] == sample_workflow_config.identifier
    assert value["params"] == sample_workflow_config.params


def test_subscribe_to_workflow_status(
    workflow_config_service, sample_workflow_status, fake_message_bridge
):
    """Test subscribing to workflow status updates for a source."""
    source_name = "source1"
    received_statuses = []

    def callback(status: WorkflowStatus) -> None:
        received_statuses.append(status)

    workflow_config_service.subscribe_to_workflow_status(source_name, callback)

    # Simulate incoming workflow status update
    status_key = WORKFLOW_STATUS.create_key(source_name=source_name)
    serialized_status = sample_workflow_status.model_dump(mode='json')
    fake_message_bridge.add_incoming_message((status_key, serialized_status))
    workflow_config_service._config_service.process_incoming_messages()

    assert len(received_statuses) == 1
    assert received_statuses[0].source_name == sample_workflow_status.source_name
    assert received_statuses[0].workflow_id == sample_workflow_status.workflow_id
    assert received_statuses[0].status == sample_workflow_status.status
    assert received_statuses[0].message == sample_workflow_status.message


def test_subscribe_to_workflow_status_different_sources(
    workflow_config_service, sample_workflow_status, fake_message_bridge
):
    """Test that workflow status subscriptions are source-specific."""
    source1_statuses = []
    source2_statuses = []

    def source1_callback(status: WorkflowStatus) -> None:
        source1_statuses.append(status)

    def source2_callback(status: WorkflowStatus) -> None:
        source2_statuses.append(status)

    workflow_config_service.subscribe_to_workflow_status("source1", source1_callback)
    workflow_config_service.subscribe_to_workflow_status("source2", source2_callback)

    # Send update for source1 only
    status_key = WORKFLOW_STATUS.create_key(source_name="source1")
    serialized_status = sample_workflow_status.model_dump(mode='json')
    fake_message_bridge.add_incoming_message((status_key, serialized_status))
    workflow_config_service._config_service.process_incoming_messages()

    assert len(source1_statuses) == 1
    assert len(source2_statuses) == 0


def test_multiple_subscribers_same_key(
    workflow_config_service, sample_workflow_status, fake_message_bridge
):
    """Test that multiple subscribers to the same key all receive updates."""
    source_name = "source1"
    received_statuses_1 = []
    received_statuses_2 = []

    def callback1(status: WorkflowStatus) -> None:
        received_statuses_1.append(status)

    def callback2(status: WorkflowStatus) -> None:
        received_statuses_2.append(status)

    workflow_config_service.subscribe_to_workflow_status(source_name, callback1)
    workflow_config_service.subscribe_to_workflow_status(source_name, callback2)

    # Send update
    status_key = WORKFLOW_STATUS.create_key(source_name=source_name)
    serialized_status = sample_workflow_status.model_dump(mode='json')
    fake_message_bridge.add_incoming_message((status_key, serialized_status))
    workflow_config_service._config_service.process_incoming_messages()

    assert len(received_statuses_1) == 1
    assert len(received_statuses_2) == 1
    assert received_statuses_1[0].workflow_id == sample_workflow_status.workflow_id
    assert received_statuses_2[0].workflow_id == sample_workflow_status.workflow_id


def test_callback_receives_existing_data_on_subscription(
    workflow_config_service, sample_workflow_status, fake_message_bridge
):
    """Test that new subscribers receive existing data immediately."""
    source_name = "source1"

    # First, set up some existing data
    status_key = WORKFLOW_STATUS.create_key(source_name=source_name)
    serialized_status = sample_workflow_status.model_dump(mode='json')
    fake_message_bridge.add_incoming_message((status_key, serialized_status))
    workflow_config_service._config_service.process_incoming_messages()

    # Now subscribe and check that callback is called immediately
    received_statuses = []

    def callback(status: WorkflowStatus) -> None:
        received_statuses.append(status)

    workflow_config_service.subscribe_to_workflow_status(source_name, callback)

    assert len(received_statuses) == 1
    assert received_statuses[0].workflow_id == sample_workflow_status.workflow_id
