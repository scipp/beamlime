# Beamlime Dashboard Design Document

## Executive Summary

The Beamlime dashboard provides real-time visualization of detector data streams, combining live data display with user-configurable processing controls. The architecture emphasizes maintainability, testability, and reusability across different dashboard variants while maintaining clear separation of concerns between data streaming, business logic, and presentation layers.

## System Architecture Overview

### Core Design Principles

1. **Dependency Injection by Convention**: Manual dependency injection without frameworks, following patterns used in related Beamlime software
2. **Clear Layer Separation**: Kafka integration, GUI presentation, and application logic remain strictly decoupled
3. **Shared Component Architecture**: Core components reusable across different dashboard server variants
4. **Testable Design**: Each layer unit-testable in isolation

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
│ Domain Layer (Pure Python)                                 │
│ ├─ Data Processing Pipeline                                 │
│ ├─ Configuration Models                                     │
│ ├─ Data Models & Validation                                │
│ └─ Processing Algorithms                                    │
├─────────────────────────────────────────────────────────────┤
│ Infrastructure Layer                                        │
│ ├─ Kafka Consumer (Data Ingestion)                         │
│ ├─ Kafka Producer (Config Publishing)                      │
│ ├─ Data Buffer Management                                   │
│ └─ External Service Clients                                 │
└─────────────────────────────────────────────────────────────┘
```

## Detailed Component Design

### 1. Domain Layer (Pure Business Logic)

**Responsibilities:**
- Data processing algorithms (1D/2D detector data transformation)
- Configuration validation and modeling
- Processing pipeline orchestration
- No external dependencies (Kafka, Dash, Plotly)

**Key Components:**

```python
# Data processing pipeline
class DetectorDataProcessor:
    def process_raw_data(self, raw_data: np.ndarray, config: ProcessingConfig) -> ProcessedData:
        """Apply user-configured processing to raw detector data."""

    def apply_weights(self, data: np.ndarray, weights: np.ndarray) -> np.ndarray:
        """Apply detector weights if enabled."""

    def compute_statistics(self, data: np.ndarray) -> DataStatistics:
        """Compute rolling statistics for display."""

# Configuration modeling
@dataclass
class ProcessingConfig:
    use_weights: bool
    update_interval_ms: int
    processing_mode: str
    threshold_values: dict[str, float]

    def validate(self) -> None:
        """Validate configuration parameters."""

# Data models
@dataclass
class DetectorData:
    timestamp: datetime
    raw_data: np.ndarray
    metadata: dict[str, Any]

@dataclass
class ProcessedData:
    processed_data: np.ndarray
    statistics: DataStatistics
    processing_info: dict[str, Any]
```

### 2. Infrastructure Layer (External Integrations)

**Responsibilities:**
- Kafka message consumption/production
- Data buffering and stream management
- External service communication
- No business logic

**Key Components:**

```python
# Kafka integration
class KafkaDataConsumer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self._consumer = KafkaConsumer(...)

    def consume_data_stream(self) -> Iterator[DetectorData]:
        """Yield detector data from Kafka stream."""

class KafkaConfigPublisher:
    def __init__(self, bootstrap_servers: str, topic: str):
        self._producer = KafkaProducer(...)

    def publish_config(self, config: ProcessingConfig) -> None:
        """Publish configuration changes to Kafka."""

# Data buffer management
class DataBuffer:
    def __init__(self, max_size: int = 1000):
        self._buffer: deque[ProcessedData] = deque(maxlen=max_size)

    def add_data(self, data: ProcessedData) -> None:
        """Add new data point to rolling buffer."""

    def get_recent_data(self, n_points: int = 100) -> list[ProcessedData]:
        """Retrieve recent data for visualization."""
```

### 3. Application Service Layer (Orchestration)

**Responsibilities:**
- Coordinate between domain and infrastructure layers
- Prepare data for presentation
- Handle application workflows
- Dependency injection coordination

**Key Components:**

```python
# Main orchestrator
class DashboardOrchestrator:
    def __init__(
        self,
        data_processor: DetectorDataProcessor,
        data_consumer: KafkaDataConsumer,
        config_publisher: KafkaConfigPublisher,
        data_buffer: DataBuffer,
    ):
        self._data_processor = data_processor
        self._data_consumer = data_consumer
        self._config_publisher = config_publisher
        self._data_buffer = data_buffer

    def start_data_stream(self) -> None:
        """Start consuming and processing data stream."""

    def update_configuration(self, config: ProcessingConfig) -> None:
        """Update processing configuration and publish to Kafka."""

# Figure preparation service
class FigureService:
    def __init__(self, data_buffer: DataBuffer):
        self._data_buffer = data_buffer

    def create_1d_plot(self, config: PlotConfig) -> go.Figure:
        """Create 1D Plotly figure from recent data."""

    def create_2d_heatmap(self, config: PlotConfig) -> go.Figure:
        """Create 2D heatmap from recent data."""

    def create_statistics_table(self) -> dict:
        """Prepare statistics data for table display."""

# Configuration service
class ConfigurationService:
    def __init__(self, config_publisher: KafkaConfigPublisher):
        self._config_publisher = config_publisher
        self._current_config = ProcessingConfig()

    def update_config(self, **kwargs) -> ProcessingConfig:
        """Update configuration with new values."""

    def get_current_config(self) -> ProcessingConfig:
        """Get current configuration state."""
```

### 4. Presentation Layer (Dash + Bootstrap)

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

        # Domain layer
        self.data_processor = DetectorDataProcessor()

        # Service layer
        self.orchestrator = DashboardOrchestrator(
            self.data_processor,
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

### Unit Testing Architecture

```python
# Domain layer tests (pure Python)
def test_detector_data_processor():
    processor = DetectorDataProcessor()
    config = ProcessingConfig(use_weights=True, update_interval_ms=100)
    raw_data = np.random.random((100, 100))

    result = processor.process_raw_data(raw_data, config)

    assert isinstance(result, ProcessedData)
    assert result.processed_data.shape == raw_data.shape

# Service layer tests (with mocked dependencies)
def test_configuration_service():
    mock_publisher = Mock(spec=KafkaConfigPublisher)
    service = ConfigurationService(mock_publisher)

    config = service.update_config(use_weights=False)

    assert config.use_weights is False
    mock_publisher.publish_config.assert_called_once_with(config)

# Integration tests (using dash[testing])
def test_dashboard_interaction(dash_duo):
    app = create_dashboard_app(test_kafka_config)
    dash_duo.start_server(app)

    # Test user interaction
    dash_duo.find_element("#use-weights-switch").click()
    dash_duo.wait_for_text_to_equal("#config-status", "Config updated")
```

### Mock Strategy for External Dependencies

```python
# Test fixtures
@pytest.fixture
def mock_kafka_consumer():
    consumer = Mock(spec=KafkaDataConsumer)
    consumer.consume_data_stream.return_value = iter([
        DetectorData(
            timestamp=datetime.now(),
            raw_data=np.random.random((100, 100)),
            metadata={}
        )
    ])
    return consumer

@pytest.fixture
def dashboard_dependencies(mock_kafka_consumer):
    """Create test dependencies with mocked external services."""
    deps = DashboardDependencies.__new__(DashboardDependencies)
    deps.data_consumer = mock_kafka_consumer
    deps.config_publisher = Mock(spec=KafkaConfigPublisher)
    # ... initialize other dependencies
    return deps
```

## Performance Considerations

### Data Streaming Optimization

- **Efficient Buffering**: Circular buffer with configurable size limits
- **Selective Updates**: Only update active plot tabs to reduce computation
- **Background Processing**: Kafka consumption in separate thread
- **Memory Management**: Automatic cleanup of old data points

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
@dataclass
class DashboardConfig:
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

This architecture provides a robust foundation for the Beamlime dashboard while maintaining the flexibility to support multiple dashboard variants. The clear separation of concerns, dependency injection patterns, and comprehensive testing strategy ensure maintainable, extensible code that aligns with the project's development principles.

Key benefits:
- **Maintainable**: Clear layer separation and dependency injection
- **Testable**: Each layer can be unit tested in isolation
- **Reusable**: Shared components across dashboard variants
- **Scalable**: Plugin architecture for future extensions
- **Professional**: Modern UI with Dash Bootstrap Components
