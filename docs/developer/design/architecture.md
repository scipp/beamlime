# Beamlime Dashboard Architecture

## Table of Contents

1. [Overview](#overview)
2. [System Context: Dashboard and Kafka Integration](#system-context-dashboard-and-kafka-integration)
3. [High-Level Architecture](#high-level-architecture)
4. [Configuration Architecture - Pydantic vs Param Separation](#configuration-architecture---pydantic-vs-param-separation)
5. [Data Flow Architecture](#data-flow-architecture)
6. [Component Architecture](#component-architecture)
7. [The BaseParamModel Mechanism](#the-baseparammodel-mechanism)
8. [MVC Pattern Analysis](#mvc-pattern-analysis)
9. [Background Threading Architecture](#background-threading-architecture)
10. [Key Architectural Principles](#key-architectural-principles)
11. [Testing Strategy](#testing-strategy)
12. [Extension Points](#extension-points)
13. [Development Principles and Technologies](#development-principles-and-technologies)

## Overview

The Beamlime dashboard is a real-time data visualization system that follows a layered architecture with clear separation of concerns between presentation, application logic, and infrastructure. The system is designed for live display of raw and processed detector data with configurable processing parameters, using dependency injection patterns for testability and maintainability.

A key architectural principle is the separation between **Pydantic models** (used for Kafka message validation and backend communication) and **Param models** (used for GUI widgets and user interaction). This separation ensures type safety across the system boundary while providing rich interactive controls.

The dashboard processes 1-D and 2-D data displayed using Plotly with update rates on the order of 1Hz. Data updates are received via Kafka streams, and user controls result in configuration updates published to Kafka topics for backend consumption.

## System Context: Dashboard and Kafka Integration

The Beamlime dashboard operates within a Kafka-based system, interacting with multiple backend services via Kafka topics for both data and configuration.

```mermaid
flowchart TD
    ConfigTopic["Beamlime Config Kafka Topic"]
    DataTopic["Beamlime Data Kafka Topic"]

    subgraph BackendServices
        MonitorData["monitor_data service"]
        DetectorData["detector_data service"]
        DataReduction["data_reduction service"]
    end

    DashboardApp["Beamlime Dashboard"]

    %% Data publishing
    MonitorData -- Publishes --> DataTopic
    DetectorData -- Publishes --> DataTopic
    DataReduction -- Publishes --> DataTopic

    %% Data consumption by dashboard
    DataTopic -- Feeds --> DashboardApp

    %% Config flows
    DashboardApp -- Publishes config --> ConfigTopic
    DashboardApp -- Consumes config --> ConfigTopic

    ConfigTopic -- Consumed by --> MonitorData
    ConfigTopic -- Consumed by --> DetectorData
    ConfigTopic -- Consumed by --> DataReduction

    DataReduction -- Publishes config (workflow specs) --> ConfigTopic
```

**Key Points:**
- Backend Services publish data streams to a single Kafka topic
- The Dashboard consumes this data topic, feeding into internal `DataService` components
- The Dashboard both publishes to and consumes from the config topic
- All backend services consume the config topic for configuration updates
- The `data_reduction` service can publish configuration messages (workflow specs, status)

## High-Level Architecture

```mermaid
graph TD
    subgraph "External Systems"
        K1[Kafka Data Streams]
        K2[Kafka Config Topic]
    end

    subgraph "Infrastructure Layer"
        KB[KafkaBridge]
        MS[MessageSource]
    end

    subgraph "Application Layer"
        CS[ConfigService]
        DS[DataServices]
        PM[Param Models]
        WC[WorkflowController]
    end

    subgraph "Presentation Layer"
        W1[Plots]
        UI[Workflow Widgets]
        W2[Config Widgets]
    end

    CS <--> WC
    DS <--> WC
    WC --> UI
    K1 --> MS
    K2 <--> KB
    MS --> DS
    DS --> W1
    KB --> CS
    CS <--> PM
    PM --> W2
    W2 --> PM
```

### Component Overview

The architecture is structured in three main layers:

- **Infrastructure Layer**: Manages Kafka integration and external message sources
- **Application Layer**: Contains business logic, orchestration, and data management
- **Presentation Layer**: Handles GUI components and user interaction

<!-- TODO: Verify if WorkflowController is implemented and how it interacts with existing components -->

## Configuration Architecture - Pydantic vs Param Separation

The dashboard implements a configuration system that maintains a clear separation between frontend widgets (using Param for interactive controls) and backend validation/communication (using Pydantic models throughout). The `ConfigService` operates entirely with Pydantic models, ensuring type safety and validation consistency.

### Configuration Flow Overview

```mermaid
graph TB
    subgraph "GUI Layer"
        PW[Param Widgets]
        PM[Param Models<br/>TOAEdgesParam]
    end

    subgraph "Configuration Service Layer"
        CS[ConfigService<br/>Pydantic-only]
        SV[Schema Validator<br/>Pydantic Models]
        MB[Message Bridge]
    end

    subgraph "Infrastructure"
        KB[KafkaBridge]
        KT[Kafka Topic]
    end

    subgraph "Backend"
        BE[Backend Services]
        PD[Pydantic Models<br/>TOAEdges]
    end

    PW <--> PM
    PM -.->|"Creates Pydantic<br/>from Param state"| CS
    CS -.->|"Validates only<br/>Pydantic models"| SV
    CS <--> MB
    MB <--> KB
    KB <--> KT
    KT <--> BE
    CS -.->|"Callbacks with<br/>Pydantic models"| PM

    classDef pydantic fill:#e1f5fe
    classDef param fill:#f3e5f5

    class PD,CS,SV pydantic
    class PM,PW param
```

### Key Architectural Design

The implementation enforces **Pydantic models throughout the ConfigService**:

1. **ConfigService Validation**: Only accepts `pydantic.BaseModel` instances via `update_config()`
2. **Schema Registration**: Requires `type[pydantic.BaseModel]` for schema registration
3. **Message Serialization**: Uses Pydantic's `model_dump(mode='json')` for Kafka messages
4. **Callback Data**: Subscribers receive validated Pydantic model instances

### Two-Way Configuration Flow

```mermaid
sequenceDiagram
    participant PW as Param Widget
    participant PM as Param Model
    participant CS as ConfigService
    participant SV as Schema Validator
    participant KB as KafkaBridge
    participant KT as Kafka Topic
    participant BE as Backend Services

    Note over PW,BE: User Changes Parameter
    PW->>PM: User input (num_edges=150)
    PM->>PM: Create Pydantic model from Param state
    PM->>CS: update_config(key, pydantic_model)
    CS->>CS: Validate isinstance(value, BaseModel)
    CS->>SV: validate(key, pydantic_model)
    CS->>KB: publish(key, model.model_dump(mode='json'))
    KB->>KT: JSON message
    KT->>BE: Backend consumes

    Note over PW,BE: Backend/Remote Updates Configuration
    BE->>KT: Updated config (JSON)
    KT->>KB: JSON message
    KB->>CS: incoming JSON message
    CS->>SV: deserialize(key, json_data) -> Pydantic model
    CS->>PM: callback(pydantic_model)
    PM->>PM: Extract dict from Pydantic model
    PM->>PW: param.update(**model.model_dump())
    PW->>PW: Widget reflects new state
```

### BaseParamModel Translation Mechanism

The `BaseParamModel` serves as a **translation layer** that:

1. **Outbound (Param → Pydantic)**: Creates Pydantic models from Param state using `self.schema.model_validate(kwargs)`
2. **Inbound (Pydantic → Param)**: Extracts data from Pydantic models using `model.model_dump()`
3. **Schema Binding**: Connects each Param model to its corresponding Pydantic schema
4. **Validation**: Ensures all data flowing through ConfigService is properly validated

```python
# BaseParamModel's key methods
def from_pydantic(self) -> Callable[..., None]:
    def update_callback(model: pydantic.BaseModel) -> None:
        self.param.update(**model.model_dump())  # Pydantic → Param
    return update_callback

# In the param.bind callback:
def set_as_pydantic(**kwargs) -> None:
    model = self.schema.model_validate(kwargs)  # Param → Pydantic
    config_service.update_config(key, model)
```

### Architectural Benefits

1. **Type Safety**: All configuration data is validated through Pydantic schemas
2. **Serialization Consistency**: Single serialization path via `model_dump(mode='json')`
3. **Backend Compatibility**: JSON messages match Pydantic model structure
4. **Frontend Flexibility**: Rich Param widgets with bounds, selectors, and validation
5. **Clear Boundaries**: Translation happens only at the BaseParamModel layer

## Data Flow Architecture

### Real-Time Data Flow

```mermaid
sequenceDiagram
    participant K as Kafka Stream
    participant MS as MessageSource
    participant O as Orchestrator
    participant DF as DataForwarder
    participant DS as DataService
    participant S as Subscribers
    participant UI as GUI Components

    K->>MS: Raw detector/monitor data
    MS->>O: Batch messages
    O->>DF: Forward with stream name
    Note over O,DF: Transaction batching
    DF->>DS: Store by DataKey
    DS->>S: Notify subscribers
    S->>UI: Update visualizations
```

<!-- TODO: Reconcile differences between subscription models in the two documents -->

## Component Architecture

### Configuration Management Layer

```mermaid
classDiagram
    class ConfigService {
        +register_schema(key, schema)
        +subscribe(key, callback)
        +update_config(key, **kwargs)
        +get_setter(key) Callable
        +process_incoming_messages()
        -MessageBridge message_bridge
        -ConfigSchemaValidator schema_validator
        -dict[K, list[Callable]] subscribers
    }

    class ConfigSchemaManager {
        +has_schema(key) bool
        +validate(key, data) dict | None
        +dict[K, type[BaseModel]] schemas
    }

    class KafkaBridge {
        +publish(key, value)
        +pop_message() tuple | None
        +start()
        +stop()
        -Queue outgoing_queue
        -Queue incoming_queue
        -Consumer consumer
        -Producer producer
    }

    class BaseParamModel {
        <<abstract>>
        +str service_name*
        +str config_key_name*
        +type[BaseModel] schema*
        +subscribe(config_service)
        +panel()*
        +param_updater() Callable
    }

    class TOAEdgesParam {
        +Number low
        +Number high
        +Integer num_edges
        +Selector unit
        +TOAEdges schema
    }

    class TOAEdges {
        <<Pydantic>>
        +float low
        +float high
        +int num_edges
        +str unit
    }

    ConfigService --> ConfigSchemaManager : uses
    ConfigService --> KafkaBridge : uses
    BaseParamModel --> ConfigService : subscribes to
    TOAEdgesParam --|> BaseParamModel
    ConfigSchemaManager --> TOAEdges : validates with
    TOAEdgesParam --> TOAEdges : references schema
```

### Data Management Layer

```mermaid
classDiagram
    class DataService {
        +dict[DataKey, DataArray] data
        +register_subscriber(subscriber)
        +transaction() context
        -notify_subscribers(keys)
        -list[DataSubscriber] subscribers
        -set[DataKey] pending_updates
        -int transaction_depth
    }

    class DataForwarder {
        +dict[str, DataService] data_services
        +forward(stream_name, value)
        +transaction() context
    }

    class DataSubscriber {
        <<abstract>>
        +set[DataKey] keys
        +trigger(store)
        +send(data)*
    }

    class ComponentDataSubscriber {
        +ComponentDataKey component_key
        +Pipe pipe
        +send(data)
    }

    class DataKey {
        +str service_name
        +str source_name
        +str key
    }

    class ComponentDataKey {
        <<abstract>>
        +str component_name
        +str view_name
        +str service_name*
        +cumulative_key() DataKey
        +current_key() DataKey
    }

    DataService --> DataSubscriber : notifies
    DataForwarder --> DataService : forwards to
    ComponentDataSubscriber --|> DataSubscriber
    DataSubscriber --> DataKey : depends on
    DataService --> DataKey : indexed by
    ComponentDataSubscriber --> ComponentDataKey : uses
```

## The BaseParamModel Mechanism

### Purpose and Role

`BaseParamModel` is a key architectural component for simple configuration widgets. It serves as a dedicated translation layer and per-widget controller, bridging the gap between:

- **Param models** (`param.Parameterized`): Used for GUI widgets and user interaction
- **Pydantic models**: Used for backend validation, serialization, and communication

This mechanism enables a clean, testable, and maintainable way to synchronize widget state with configuration state, without leaking infrastructure details into the presentation layer.

### How It Works

- Each simple configuration widget is backed by a `BaseParamModel` subclass
- The `BaseParamModel`:
  - Registers the relevant Pydantic schema with `ConfigService`
  - Subscribes to config updates for its key, updating the widget state when changes arrive
  - Propagates user changes from the widget to `ConfigService` by translating Param state to a Pydantic model

```mermaid
sequenceDiagram
    participant ConfigService
    participant BaseParamModel
    participant PanelWidget

    ConfigService->>BaseParamModel: Notify config update
    BaseParamModel->>PanelWidget: Update widget state
    PanelWidget->>BaseParamModel: User changes widget
    BaseParamModel->>ConfigService: Update config
```

### Architectural Implications

- **Localized Coupling**: `BaseParamModel` knows about both Param and Pydantic models, but this coupling is intentional and limited to the translation layer
- **No Architectural Problem**: This is not problematic coupling, but a necessary and well-encapsulated translation between two distinct model types
- **Testability**: The translation logic is isolated and can be tested independently
- **Extensibility**: More complex workflows can use a centralized controller, while simple controls benefit from this lightweight mechanism

## MVC Pattern Analysis

### Mapping to MVC

| MVC Component | Beamlime Implementation |
|---------------|------------------------|
| Model         | ConfigService, DataService |
| View          | Panel Widgets              |
| Controller    | WorkflowController, BaseParamModel (for simple controls) |

```mermaid
flowchart LR
    Model["Model<br/>(ConfigService, DataService)"]
    Controller["Controller<br/>(WorkflowController, BaseParamModel)"]
    View["View<br/>(Panel Widgets)"]

    View <--> Controller
    Controller <--> Model
```

### Subscription and Notification Flow

```mermaid
sequenceDiagram
    participant ConfigService
    participant DataService
    participant WorkflowController
    participant BaseParamModel
    participant PanelWidgets

    ConfigService->>WorkflowController: Notify config update
    DataService->>WorkflowController: Notify data update
    WorkflowController->>PanelWidgets: Notify relevant update
    PanelWidgets->>WorkflowController: User actions (start/stop workflow)
    WorkflowController->>ConfigService: Update config
    WorkflowController->>DataService: Update data (e.g., cleanup)

    ConfigService->>BaseParamModel: Notify config update (simple controls)
    BaseParamModel->>PanelWidgets: Notify widget update
    PanelWidgets->>BaseParamModel: User changes widget (simple controls)
    BaseParamModel->>ConfigService: Update config (simple controls)
```

### Analysis

**Strengths:**
- Clear separation of concerns with well-defined responsibilities
- Testability through controller and widget isolation using fakes
- Maintainability with centralized business logic
- Extensibility for future requirements
- Efficient handling of simple controls via `BaseParamModel`

**Potential Pitfalls:**
- Controller bloat as it mediates more services
- State synchronization challenges between controller and services
- Subscription complexity with multiple layers

**Anti-Patterns Avoided:**
- No leaky abstractions between views and services
- No tight coupling between controllers and specific GUI frameworks
- No direct service access from views

## Background Threading Architecture

### KafkaBridge Threading Model

```mermaid
sequenceDiagram
    participant GUI as GUI Thread
    participant KB as KafkaBridge
    participant BT as Background Thread
    participant K as Kafka

    Note over GUI,K: Startup
    GUI->>KB: start()
    KB->>BT: spawn thread
    BT->>K: subscribe to topic

    Note over GUI,K: Publishing (Non-blocking)
    GUI->>KB: publish(key, value)
    KB->>KB: queue.put()
    BT->>KB: queue.get()
    BT->>K: produce message

    Note over GUI,K: Consuming (Batched)
    K->>BT: poll messages
    BT->>KB: incoming_queue.put()
    GUI->>KB: process_incoming_messages()
    KB->>GUI: pop_message() × N
```

### Message Processing Strategy

The KafkaBridge implements several optimizations:

1. **Batched Processing**: Consumes up to `max_batch_size` messages per poll
2. **Timed Polling**: Only checks for incoming messages at specified intervals
3. **Queue-based Communication**: Non-blocking queues between GUI and background threads
4. **Smart Idle Handling**: Minimal CPU usage when no messages are available

## Key Architectural Principles

### 1. Pydantic/Param Separation

**Pydantic Models** (Backend/Validation):
```python
class TOAEdges(BaseModel):
    low: float
    high: float  
    num_edges: int
    unit: str
```

**Param Models** (Frontend/GUI):
```python
class TOAEdgesParam(BaseParamModel):
    low = param.Number(default=0.0)
    high = param.Number(default=72_000.0)
    num_edges = param.Integer(default=100, bounds=(1, 1000))
    unit = param.Selector(default='us', objects=['ns', 'us', 'ms', 's'])

    @property
    def schema(self) -> type[TOAEdges]:
        return TOAEdges
```

### 2. Two-Way Configuration Binding

- **GUI → Backend**: User changes widget → Param model → ConfigService → Kafka → Backend
- **Backend → GUI**: Backend update → Kafka → ConfigService → Param model → Widget

### 3. Transaction Support and Message Batching

```python
with data_service.transaction():
    # Multiple updates batched together
    data_service[key1] = value1
    data_service[key2] = value2
    # Subscribers notified only once at end
```

### 4. Dependency Injection

```python
# Configuration dependencies
config_service = ConfigService(
    message_bridge=kafka_bridge,
    schema_validator=schema_manager
)

# Data flow dependencies  
orchestrator = Orchestrator(
    message_source=message_source,
    forwarder=data_forwarder
)
```

### 5. Publisher-Subscriber with Transaction Support

```mermaid
graph LR
    DS[DataService] --> S1[Monitor1 Subscriber]
    DS --> S2[Monitor2 Subscriber]  
    DS --> S3[Status Subscriber]
    S1 --> P1[Monitor1 Plot]
    S2 --> P2[Monitor2 Plot]
    S3 --> P3[Status Bar]

    style DS fill:#f9f,stroke:#333,stroke-width:2px
```

## Testing Strategy

### Architecture for Testability

```mermaid
graph TB
    subgraph "Production"
        P1[KafkaBridge]
        P2[Real MessageSource]
        P3[ConfigSchemaManager]
    end

    subgraph "Testing"
        T1[FakeMessageBridge]
        T2[Fake MessageSource]
        T3[Mock SchemaValidator]
    end

    subgraph "Application Logic"
        CS[ConfigService]
        O[Orchestrator]
    end

    P1 -.-> CS
    T1 -.-> CS
    P2 -.-> O
    T2 -.-> O
    P3 -.-> CS
    T3 -.-> CS
```

The architecture allows easy substitution of infrastructure components:

- `FakeMessageBridge` and `LoopbackMessageBridge` for testing configuration flow
- Mock `MessageSource` for testing data processing  
- `DataService` can be tested with mock subscribers
- Pydantic validation can be tested independently of Param widgets

## Extension Points

### Adding New Configuration Parameters

1. **Create Pydantic Model** (Backend validation):
```python
class NewParam(BaseModel):
    value: float
    enabled: bool
```

2. **Create Param Model** (GUI widget):
```python  
class NewParamWidget(BaseParamModel):
    value = param.Number(default=1.0)
    enabled = param.Boolean(default=True)

    @property
    def schema(self) -> type[NewParam]:
        return NewParam
```

3. **Subscribe to ConfigService**:
```python
widget.subscribe(config_service)
```

### Adding New Data Types

1. Create new `DataKey` subclass
2. Implement corresponding `DataSubscriber`
3. Register subscriber with appropriate `DataService`

### Adding New Visualizations

1. Create new subscriber implementing `DataSubscriber`
2. Register with appropriate `DataService`  
3. Implement visualization using Holoviews/Panel

## Development Principles and Technologies

### Core Principles

- Well architected, maintainable, and testable code using modern Python
- Dependency injection patterns implemented by hand for better testability
- Separation of Kafka integration and GUI presentation from application logic
- Architecture designed for easy testing using fakes rather than mocks
- Layered architecture with clear distinction between presentation, application service, and infrastructure layers
- Support for multiple app versions while reusing core components

### Technology Stack

- **Kafka Integration**: `confluence_kafka` for message streaming
- **Serialization**: `pydantic` for message validation and type safety
- **GUI Framework**: `panel` and `holoviews` for interactive dashboards
- **Data Visualization**: Plotly for 1-D and 2-D data display
- **Testing**: `pytest` for comprehensive test coverage
- **Type System**: Modern Python with type hints using `list`/`dict` and `A | B` syntax

### Architectural Patterns

- Publisher-subscriber pattern for data and configuration updates
- Transaction pattern for batched operations
- Adapter pattern for Pydantic/Param translation
- Dependency injection for testability and modularity
- Layered architecture for separation of concerns

---

This architecture provides a robust foundation for the live data dashboard while maintaining clear separation of concerns, comprehensive validation, and extensive testability. The Pydantic/Param separation ensures type safety across system boundaries while enabling rich interactive controls.