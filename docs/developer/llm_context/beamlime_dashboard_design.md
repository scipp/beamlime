# Beamlime Dashboard Design Document

## Executive Summary

The Beamlime dashboard provides real-time visualization of processed detector data streams, combining live data display with user-configurable processing controls. The dashboard consumes pre-processed data from Kafka microservices and publishes configuration messages to control upstream processing. The architecture emphasizes maintainability, testability, and reusability across different dashboard variants while maintaining clear separation of concerns between data streaming and presentation layers.

## System Architecture Overview

### Core Design Principles

1. **Dependency Injection by Convention**: Manual dependency injection without frameworks, following patterns used in related Beamlime software
2. **Clear Layer Separation**: Kafka integration and GUI presentation remain strictly decoupled
3. **Shared Component Architecture**: Core components reusable across different dashboard server variants
4. **Testable Design**: Each layer unit-testable in isolation
5. **Configuration as Messages**: Pydantic models for all configuration and control message serialization

### Architectural Layers

```
┌─────────────────────────────────────────────────────────────┐
│ Presentation Layer (Dash + Bootstrap Components)           │
│ ├─ Layout Components (DBC-based)                           │
│ ├─ Callback Controllers (minimal logic)                    │
│ └─ UI State Management                                      │
├─────────────────────────────────────────────────────────────┤
│ Application Service Layer                                   │
│ ├─ Dashboard Orchestrator                                   │
│ ├─ Configuration Service                                    │
│ ├─ Data Aggregation Service                                │
│ └─ Figure Preparation Service                               │
├─────────────────────────────────────────────────────────────┤
│ Infrastructure Layer                                        │
│ ├─ Kafka Consumer (Data Ingestion)                         │
│ ├─ Kafka Producer (Config Publishing)                      │
│ ├─ Data Buffer Management                                   │
│ └─ External Service Clients                                 │
└─────────────────────────────────────────────────────────────┘
```

**Note on Layer Architecture**: The dashboard service primarily orchestrates data visualization and user interaction. Unlike a full-featured application, there is minimal domain logic beyond Pydantic models for message serialization. The Application Service Layer handles orchestration, data aggregation for visualization, and configuration management, while the Infrastructure Layer manages external integrations. A separate Domain Layer would add unnecessary complexity for this focused service.

## Detailed Component Design

### 1. Application Service Layer (Orchestration & Business Logic)

**Responsibilities:**
- Orchestrate data flow from Kafka to visualization
- Manage configuration state and publishing
- Prepare data for presentation (aggregation, formatting)
- Handle application workflows
- Pydantic models for message serialization

**Key Components:**

```python
# Pydantic models for configuration
class ProcessingConfig(BaseModel):
    use_weights: bool = False
    update_interval_ms: int = 1000
    processing_mode: str = "standard"
    threshold_values: dict[str, float] = {}

    class Config:
        extra = "forbid"

class DetectorData(BaseModel):
    timestamp: datetime
    processed_data: list[list[float]]  # Pre-processed by upstream services
    metadata: dict[str, Any]
    processing_info: dict[str, Any]

class DataStatistics(BaseModel):
    mean: float
    std: float
    min_value: float
    max_value: float
    total_counts: int

# Main orchestrator
class DashboardOrchestrator:
    def __init__(
        self,
        data_consumer: KafkaDataConsumer,
        config_publisher: KafkaConfigPublisher,
        data_buffer: DataBuffer,
    ):
        self._data_consumer = data_consumer
        self._config_publisher = config_publisher
        self._data_buffer = data_buffer

    def start_data_stream(self) -> None:
        """Start consuming processed data stream from Kafka."""

    def update_configuration(self, config: ProcessingConfig) -> None:
        """Update processing configuration and publish to Kafka."""

# Figure preparation service
class FigureService:
    def __init__(self, data_buffer: DataBuffer):
        self._data_buffer = data_buffer

    def create_1d_plot(self, config: PlotConfig) -> go.Figure:
        """Create 1D Plotly figure from recent processed data."""

    def create_2d_heatmap(self, config: PlotConfig) -> go.Figure:
        """Create 2D heatmap from recent processed data."""

    def create_statistics_table(self) -> dict:
        """Prepare statistics data for table display."""

# Configuration service
class ConfigurationService:
    def __init__(self, config_publisher: KafkaConfigPublisher):
        self._config_publisher = config_publisher
        self._current_config = ProcessingConfig()

    def update_config(self, **kwargs) -> ProcessingConfig:
        """Update configuration with new values and validate with Pydantic."""
        config_dict = self._current_config.dict()
        config_dict.update(kwargs)
        self._current_config = ProcessingConfig(**config_dict)
        return self._current_config

    def get_current_config(self) -> ProcessingConfig:
        """Get current configuration state."""
        return self._current_config
```

### 2. Infrastructure Layer (External Integrations)

**Responsibilities:**
- Kafka message consumption/production with Pydantic serialization
- Data buffering and stream management
- External service communication
- No business logic

**Key Components:**

```python
# Kafka integration with Pydantic
class KafkaDataConsumer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self._consumer = KafkaConsumer(...)

    def consume_data_stream(self) -> Iterator[DetectorData]:
        """Yield processed detector data from Kafka stream, deserializing with Pydantic."""
        for message in self._consumer:
            try:
                data_dict = json.loads(message.value.decode('utf-8'))
                yield DetectorData(**data_dict)
            except ValidationError as e:
                logger.warning("Invalid message format: %s", e)

class KafkaConfigPublisher:
    def __init__(self, bootstrap_servers: str, topic: str):
        self._producer = KafkaProducer(...)

    def publish_config(self, config: ProcessingConfig) -> None:
        """Publish configuration changes to Kafka using Pydantic serialization."""
        config_json = config.json()
        self._producer.send(self._topic, config_json.encode('utf-8'))

# Data buffer management
class DataBuffer:
    def __init__(self, max_size: int = 1000):
        self._buffer: deque[DetectorData] = deque(maxlen=max_size)

    def add_data(self, data: DetectorData) -> None:
        """Add new processed data point to rolling buffer."""
        self._buffer.append(data)

    def get_recent_data(self, n_points: int = 100) -> list[DetectorData]:
        """Retrieve recent processed data for visualization."""
        return list(self._buffer)[-n_points:]
```

### 3. Presentation Layer (Dash + Bootstrap)

**Responsibilities:**
- UI layout using Dash Bootstrap Components
- Callback registration and handling
- User interaction management
- Minimal business logic (delegation to services)

**Key Components:**

```python
# Layout factory
def create_dashboard_layout(config_service: ConfigurationService) -> dbc.Container:
    """Create main dashboard layout using DBC components."""
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                create_control_panel(config_service),
            ], width=3),
            dbc.Col([
                create_plot_tabs(),
            ], width=9),
        ]),
        create_status_bar(),
    ])

def create_control_panel(config_service: ConfigurationService) -> dbc.Card:
    """Create control panel with user configuration options."""
    current_config = config_service.get_current_config()

    return dbc.Card([
        dbc.CardHeader("Processing Controls"),
        dbc.CardBody([
            dbc.Label("Update Rate (Hz)"),
            dcc.Slider(
                id='update-rate-slider',
                min=8, max=13, step=0.5,
                value=np.log2(1000 / current_config.update_interval_ms),
                marks={i: {'label': f'{2**i} Hz'} for i in range(8, 14)},
            ),
            dbc.Switch(
                id="use-weights-switch",
                label="Apply detector weights",
                value=current_config.use_weights,
            ),
            # Additional controls...
        ])
    ])

def create_plot_tabs() -> dbc.Tabs:
    """Create tabbed interface for different plot types."""
    return dbc.Tabs([
        dbc.Tab(
            dcc.Graph(id="1d-plot"),
            label="1D Data",
            tab_id="1d-tab"
        ),
        dbc.Tab(
            dcc.Graph(id="2d-heatmap"),
            label="2D Heatmap",
            tab_id="2d-tab"
        ),
        dbc.Tab(
            html.Div(id="statistics-table"),
            label="Statistics",
            tab_id="stats-tab"
        ),
    ], id="plot-tabs", active_tab="1d-tab")

# Callback controllers
class CallbackController:
    def __init__(
        self,
        orchestrator: DashboardOrchestrator,
        figure_service: FigureService,
        config_service: ConfigurationService,
    ):
        self._orchestrator = orchestrator
        self._figure_service = figure_service
        self._config_service = config_service

    def register_callbacks(self, app: Dash) -> None:
        """Register all dashboard callbacks."""

        @app.callback(
            Output("1d-plot", "figure"),
            Input("interval-component", "n_intervals"),
            State("plot-tabs", "active_tab"),
        )
        def update_1d_plot(n_intervals: int, active_tab: str) -> go.Figure:
            if active_tab != "1d-tab":
                return dash.no_update
            return self._figure_service.create_1d_plot(PlotConfig())

        @app.callback(
            Output("config-status", "children"),
            Input("use-weights-switch", "value"),
            Input("update-rate-slider", "value"),
        )
        def update_configuration(use_weights: bool, update_rate: float) -> str:
            config = self._config_service.update_config(
                use_weights=use_weights,
                update_interval_ms=int(1000 / (2 ** update_rate))
            )
            self._orchestrator.update_configuration(config)
            return f"Config updated: {config.update_interval_ms}ms, weights={use_weights}"
```

## Dependency Injection Architecture

### Manual Dependency Injection Pattern

Following the project's convention of manual dependency injection:

```python
# Dependency container
class DashboardDependencies:
    def __init__(self, kafka_config: KafkaConfig):
        # Infrastructure layer
        self.data_consumer = KafkaDataConsumer(
            kafka_config.bootstrap_servers,
            kafka_config.data_topic
        )
        self.config_publisher = KafkaConfigPublisher(
            kafka_config.bootstrap_servers,
            kafka_config.config_topic
        )
        self.data_buffer = DataBuffer(max_size=1000)

        # Service layer
        self.orchestrator = DashboardOrchestrator(
            self.data_consumer,
            self.config_publisher,
            self.data_buffer,
        )
        self.figure_service = FigureService(self.data_buffer)
        self.config_service = ConfigurationService(self.config_publisher)

        # Presentation layer
        self.callback_controller = CallbackController(
            self.orchestrator,
            self.figure_service,
            self.config_service,
        )

# Application factory
def create_dashboard_app(kafka_config: KafkaConfig) -> Dash:
    """Create configured Dash application."""
    deps = DashboardDependencies(kafka_config)

    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    app.layout = create_dashboard_layout(deps.config_service)

    deps.callback_controller.register_callbacks(app)
    deps.orchestrator.start_data_stream()

    return app
```

## Multi-Version Architecture

### Shared Component Strategy

To support multiple dashboard variants while reusing components:

```python
# Base dashboard factory
class BaseDashboardFactory:
    def __init__(self, kafka_config: KafkaConfig):
        self.kafka_config = kafka_config

    def create_dependencies(self) -> DashboardDependencies:
        """Create standard dependency configuration."""
        return DashboardDependencies(self.kafka_config)

    def create_base_layout(self, deps: DashboardDependencies) -> dbc.Container:
        """Create base layout - override in subclasses."""
        return create_dashboard_layout(deps.config_service)

# Specialized dashboard variants
class ExperimentDashboardFactory(BaseDashboardFactory):
    def create_base_layout(self, deps: DashboardDependencies) -> dbc.Container:
        """Create experiment-specific layout with additional controls."""
        base_layout = super().create_base_layout(deps)
        # Add experiment-specific components
        return enhance_layout_for_experiment(base_layout)

class MonitoringDashboardFactory(BaseDashboardFactory):
    def create_dependencies(self) -> DashboardDependencies:
        """Create monitoring-specific dependencies."""
        deps = super().create_dependencies()
        # Add monitoring-specific services
        deps.monitoring_service = MonitoringService(deps.data_buffer)
        return deps
```

## Testing Strategy

### Unit Testing Architecture with Fakes

```python
# Fake implementations instead of mocks
class FakeKafkaDataConsumer:
    def __init__(self, test_data: list[DetectorData] | None = None):
        self._test_data = test_data or []
        self._index = 0

    def consume_data_stream(self) -> Iterator[DetectorData]:
        """Yield test data for predictable testing."""
        while self._index < len(self._test_data):
            yield self._test_data[self._index]
            self._index += 1

class FakeKafkaConfigPublisher:
    def __init__(self):
        self.published_configs: list[ProcessingConfig] = []

    def publish_config(self, config: ProcessingConfig) -> None:
        """Record published configurations for test verification."""
        self.published_configs.append(config)

# Service layer tests with fakes
def test_configuration_service():
    fake_publisher = FakeKafkaConfigPublisher()
    service = ConfigurationService(fake_publisher)

    config = service.update_config(use_weights=False)

    assert config.use_weights is False
    assert len(fake_publisher.published_configs) == 0  # Not published until orchestrator calls

def test_dashboard_orchestrator():
    test_data = [
        DetectorData(
            timestamp=datetime.now(),
            processed_data=[[1.0, 2.0], [3.0, 4.0]],
            metadata={},
            processing_info={}
        )
    ]
    fake_consumer = FakeKafkaDataConsumer(test_data)
    fake_publisher = FakeKafkaConfigPublisher()
    buffer = DataBuffer()

    orchestrator = DashboardOrchestrator(fake_consumer, fake_publisher, buffer)
    config = ProcessingConfig(use_weights=True)

    orchestrator.update_configuration(config)

    assert len(fake_publisher.published_configs) == 1
    assert fake_publisher.published_configs[0].use_weights is True

# Pydantic model validation tests
def test_processing_config_validation():
    # Valid configuration
    config = ProcessingConfig(
        use_weights=True,
        update_interval_ms=500,
        processing_mode="advanced"
    )
    assert config.use_weights is True

    # Invalid configuration should raise ValidationError
    with pytest.raises(ValidationError):
        ProcessingConfig(update_interval_ms="invalid")

# Integration tests with fakes
def test_dashboard_interaction(dash_duo):
    # Create test dependencies with fakes
    fake_consumer = FakeKafkaDataConsumer([
        DetectorData(
            timestamp=datetime.now(),
            processed_data=[[1.0, 2.0], [3.0, 4.0]],
            metadata={},
            processing_info={}
        )
    ])
    fake_publisher = FakeKafkaConfigPublisher()

    # Create app with fake dependencies
    app = create_test_dashboard_app(fake_consumer, fake_publisher)
    dash_duo.start_server(app)

    # Test user interaction
    dash_duo.find_element("#use-weights-switch").click()
    dash_duo.wait_for_text_to_equal("#config-status", "Config updated")

    # Verify configuration was published
    assert len(fake_publisher.published_configs) > 0
```

### Test Fixtures with Fakes

```python
# Test fixtures
@pytest.fixture
def fake_kafka_consumer():
    return FakeKafkaDataConsumer([
        DetectorData(
            timestamp=datetime.now(),
            processed_data=[[1.0, 2.0], [3.0, 4.0]],
            metadata={},
            processing_info={}
        )
    ])

@pytest.fixture
def fake_kafka_publisher():
    return FakeKafkaConfigPublisher()

@pytest.fixture
def dashboard_dependencies(fake_kafka_consumer, fake_kafka_publisher):
    """Create test dependencies with fakes."""
    deps = DashboardDependencies.__new__(DashboardDependencies)
    deps.data_consumer = fake_kafka_consumer
    deps.config_publisher = fake_kafka_publisher
    deps.data_buffer = DataBuffer()
    # Initialize services with fakes
    deps.orchestrator = DashboardOrchestrator(
        deps.data_consumer,
        deps.config_publisher,
        deps.data_buffer,
    )
    # ...existing code...
    return deps
```

## Performance Considerations

### Data Streaming Optimization

- **Efficient Buffering**: Circular buffer with configurable size limits
- **Selective Updates**: Only update active plot tabs to reduce computation
- **Background Processing**: Kafka consumption in separate thread
- **Memory Management**: Automatic cleanup of old data points
- **Pydantic Performance**: Use `parse_obj` for better performance when needed

### UI Responsiveness

- **Client-side Callbacks**: Use for simple transformations (filtering, scaling)
- **Debounced Updates**: Prevent excessive callback triggering
- **Progressive Loading**: Show loading states for long operations
- **Efficient Plotly Updates**: Use `extendData` for streaming plots

## Deployment Architecture

### Server Variants

Each dashboard variant runs as a separate server process:

```bash
# Experiment dashboard
python -m beamlime.dashboard.experiment --kafka-servers=localhost:9092

# Monitoring dashboard  
python -m beamlime.dashboard.monitoring --kafka-servers=localhost:9092

# Development dashboard
python -m beamlime.dashboard.dev --kafka-servers=localhost:9092
```

### Configuration Management

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
        """Load configuration from environment variables."""
        return cls(
            kafka_bootstrap_servers=os.getenv('KAFKA_SERVERS', 'localhost:9092'),
            kafka_data_topic=os.getenv('KAFKA_DATA_TOPIC', 'detector-data'),
            kafka_config_topic=os.getenv('KAFKA_CONFIG_TOPIC', 'detector-config'),
        )
```

## Future Extensibility

### Plugin Architecture

```python
# Plugin interface
class DashboardPlugin(ABC):
    @abstractmethod
    def extend_layout(self, base_layout: dbc.Container) -> dbc.Container:
        """Extend base layout with plugin components."""

    @abstractmethod
    def register_callbacks(self, app: Dash, deps: DashboardDependencies) -> None:
        """Register plugin-specific callbacks."""

# Plugin registration
class PluginRegistry:
    def __init__(self):
        self._plugins: list[DashboardPlugin] = []

    def register(self, plugin: DashboardPlugin) -> None:
        self._plugins.append(plugin)

    def apply_to_app(self, app: Dash, deps: DashboardDependencies) -> None:
        for plugin in self._plugins:
            plugin.register_callbacks(app, deps)
```

## Conclusion

This architecture provides a focused, maintainable foundation for the Beamlime dashboard service. By removing data processing responsibilities and leveraging Pydantic for message serialization, the design becomes cleaner and more focused on its core responsibility: visualizing processed data and managing configuration.

Key benefits:
- **Focused Responsibility**: Dashboard handles visualization and configuration only
- **Type Safety**: Pydantic models ensure message validation and serialization
- **Testable**: Fake implementations provide predictable, controllable testing
- **Maintainable**: Clear separation between orchestration and infrastructure
- **Reusable**: Shared components across dashboard variants
- **Professional**: Modern UI with Dash Bootstrap Components

The simplified layer architecture reflects the focused nature of the dashboard service while maintaining clear separation of concerns and testability.
