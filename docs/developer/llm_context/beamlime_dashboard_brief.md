# Beamlime Dashboard Design Brief

## Overview

Real-time dashboard for visualizing processed detector data streams. Consumes pre-processed data from Kafka and publishes configuration messages to control upstream processing.

**Key Principles:**
- Manual dependency injection (no frameworks)
- Clear layer separation (Kafka ↔ GUI decoupled)
- Shared components across dashboard variants
- Testable with fakes (not mocks)
- Pydantic models for all message serialization

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│ Presentation Layer (Dash + Bootstrap Components)           │
│ ├─ Layout Components, Callbacks, UI State                  │
├─────────────────────────────────────────────────────────────┤
│ Application Service Layer                                   │
│ ├─ Dashboard Orchestrator, Configuration Service           │
│ ├─ Data Aggregation Service, Figure Preparation Service    │
├─────────────────────────────────────────────────────────────┤
│ Infrastructure Layer                                        │
│ ├─ Kafka Consumer/Producer, Data Buffer, External Clients  │
└─────────────────────────────────────────────────────────────┘
```

**Note:** Minimal domain logic - primarily orchestration and visualization. Application Service Layer handles orchestration and data preparation for visualization.

## Key Components

### Application Service Layer

**DashboardOrchestrator** - Main coordinator
- Orchestrates data flow from Kafka to visualization
- Manages configuration state and publishing
- Controls application workflows

**ConfigurationService** - Configuration management
- Updates processing configuration with Pydantic validation
- Publishes configuration changes via Kafka

**FigureService** - Plot preparation
- Creates Plotly figures from processed data
- Handles 1D plots, 2D heatmaps, statistics tables

### Infrastructure Layer

**KafkaDataConsumer/KafkaConfigPublisher** - Message handling
- Consumes processed data with Pydantic deserialization
- Publishes configuration updates

**DataBuffer** - Stream management
- Rolling buffer for recent processed data
- Configurable size limits

### Presentation Layer

**Layout Factory Functions** - UI construction
```python
def create_dashboard_layout(config_service: ConfigurationService) -> dbc.Container:
    # DBC-based layout with control panel and plot tabs

def create_control_panel(config_service: ConfigurationService) -> dbc.Card:
    # User configuration controls (sliders, switches)
```

**CallbackController** - User interaction
```python
class CallbackController:
    def register_callbacks(self, app: Dash) -> None:
        # Register plot updates and configuration callbacks
```

## Dependency Injection Pattern

```python
class DashboardDependencies:
    def __init__(self, kafka_config: KafkaConfig):
        # Infrastructure layer
        self.data_consumer = KafkaDataConsumer(...)
        self.config_publisher = KafkaConfigPublisher(...)
        self.data_buffer = DataBuffer(...)

        # Service layer
        self.orchestrator = DashboardOrchestrator(...)
        self.figure_service = FigureService(...)
        self.config_service = ConfigurationService(...)

        # Presentation layer
        self.callback_controller = CallbackController(...)

def create_dashboard_app(kafka_config: KafkaConfig) -> Dash:
    deps = DashboardDependencies(kafka_config)
    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    app.layout = create_dashboard_layout(deps.config_service)
    deps.callback_controller.register_callbacks(app)
    return app
```

## Multi-Version Support

**Shared Component Strategy:**
```python
class BaseDashboardFactory:
    def create_dependencies(self) -> DashboardDependencies:
        return DashboardDependencies(self.kafka_config)

    def create_base_layout(self, deps: DashboardDependencies) -> dbc.Container:
        # Override in subclasses for variant-specific layouts

class ExperimentDashboardFactory(BaseDashboardFactory):
    def create_base_layout(self, deps: DashboardDependencies) -> dbc.Container:
        # Add experiment-specific components
```

## Testing Strategy

**Use Fakes Instead of Mocks:**
```python
class FakeKafkaDataConsumer:
    def __init__(self, test_data: list[DetectorData] | None = None):
        self._test_data = test_data or []

    def consume_data_stream(self) -> Iterator[DetectorData]:
        # Yield predictable test data

class FakeKafkaConfigPublisher:
    def __init__(self):
        self.published_configs: list[ProcessingConfig] = []

    def publish_config(self, config: ProcessingConfig) -> None:
        self.published_configs.append(config)
```

**Test Fixtures:**
```python
@pytest.fixture
def dashboard_dependencies(fake_kafka_consumer, fake_kafka_publisher):
    # Create test dependencies with fakes
    deps = DashboardDependencies.__new__(DashboardDependencies)
    deps.data_consumer = fake_kafka_consumer
    deps.config_publisher = fake_kafka_publisher
    # Initialize services with fakes
    return deps
```

## Performance Optimization

**Data Streaming:**
- Circular buffer with configurable size limits
- Only update active plot tabs
- Background Kafka consumption thread
- Automatic cleanup of old data

**UI Responsiveness:**
- Client-side callbacks for simple transformations
- Debounced updates to prevent excessive triggering
- Progressive loading states
- Efficient Plotly updates with `extendData`

## Deployment

**Server Variants:**
```bash
# Different dashboard types as separate processes
python -m beamlime.dashboard.experiment --kafka-servers=localhost:9092
python -m beamlime.dashboard.monitoring --kafka-servers=localhost:9092
python -m beamlime.dashboard.dev --kafka-servers=localhost:9092
```

**Configuration:**
```python
class DashboardConfig(BaseModel):
    kafka_bootstrap_servers: str
    kafka_data_topic: str
    kafka_config_topic: str
    update_interval_ms: int = 1000
    buffer_size: int = 1000
    debug_mode: bool = False

    @classmethod
    def from_env(cls) -> 'DashboardConfig':
        # Load from environment variables
```

## Key Design Patterns

1. **Pydantic Models** - All Kafka messages use Pydantic for validation/serialization
2. **Manual DI** - Constructor injection without frameworks
3. **Layer Isolation** - Each layer testable in isolation
4. **Fake Objects** - Predictable testing without mocks
5. **Factory Pattern** - Dashboard variants through specialized factories
6. **Component Reuse** - Shared services across dashboard types

## Extension Points

- **Plugin Interface** for dashboard extensions
- **Custom Layout Factories** for new dashboard variants
- **Additional Services** injected through dependency container
- **Custom Kafka Topics** for specialized data streams
