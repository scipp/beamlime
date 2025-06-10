# Beamlime Dashboard

## Executive Summary

The Beamlime dashboard is used by users to follow a "live" display of the raw detector data as well as processed detector data.
Data is typically 1-D or 2-D, displayed using Plotly.
Updates rates are on the order of 1Hz.

Data updates are received via a Kafka stream, which is consumed by the dashboard application.

The user of the dashboard has simple control over the data processing.
This is done via a few simple controls, such as sliders, checkboxes, and buttons.
Controls result in config updates published to a Kafka topic, which the backend consumes and applies to the data processing.

## Development Principles

- We write well architected, maintainable, and testable code.
- We are using modern Python with good coding and development practices.
- Related software is written using dependency injection patterns (without a special framework, just by hand), so this may be a good match for some of the architecture here as well.
- We keep Kafka integration and GUI presentation separate from the rest of the application logic.
- We architect the application in a way that allows us to develop different versions of the app while reusing most of the components.
  This will be run as separate servers, so simply sharing most Python code is sufficient.