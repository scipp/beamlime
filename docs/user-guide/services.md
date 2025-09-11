# Services

## Overview

### Main services

The following main services are available:

```sh
python -m ess.livedata.services.monitor_data --instrument dummy
python -m ess.livedata.services.detector_data --instrument dummy
python -m ess.livedata.services.data_reduction --instrument dummy
python -m ess.livedata.services.timeseries --instrument dummy
```

For testing, each of these should be run with the `--dev` argument.
This will run the services with a simplified topic structure and make them compatible with the fake data services [below](#fake-data-services).

Note also the `--sink png` argument, which will save the outputs as PNG files instead of publishing them to Kafka.
This allows for testing the service outputs without running the dashboard.

### Dashboard Services

#### Development Mode

The dashboard services can be run in development mode using:

```sh
python -m ess.livedata.dashboard.monitors --instrument dummy
python -m ess.livedata.dashboard.detectors --instrument dummy
python -m ess.livedata.dashboard.reduction --instrument dummy
```

#### Production Mode

The dashboard services can be run in production mode using gunicorn:

```sh
# Monitors dashboard (runs on port 5007)
LIVEDATA_INSTRUMENT=dummy gunicorn ess.livedata.dashboard.monitors_wsgi:application

# Detectors dashboard (runs on port 5008)
LIVEDATA_INSTRUMENT=dummy gunicorn ess.livedata.dashboard.detectors_wsgi:application

# Reduction dashboard (runs on port 5009)
LIVEDATA_INSTRUMENT=dummy gunicorn ess.livedata.dashboard.reduction_wsgi:application
```

Navigate to `http://localhost:5007` for the monitors dashboard, `http://localhost:5008` for the detectors dashboard, or `http://localhost:5009` for the reduction dashboard.

All three dashboards can run simultaneously on their respective ports.

### Fake data services

The following fake data services are available:

```sh
python -m ess.livedata.services.fake_monitors --mode ev44 --instrument dummy
python -m ess.livedata.services.fake_detectors --instrument dummy
python -m ess.livedata.services.fake_logdata --instrument dummy
```

## Example: Running the monitor data service

Services can be found in `ess.livedata.services`.
Configuration is in `ess.livedata.config.defaults`.
By default the files with the `dev` suffix are used.
Set `LIVEDATA_ENV` to, e.g., `staging` to use the `staging` configuration.

For a local demo, run the fake monitor data producer:

```sh
python -m ess.livedata.services.fake_monitors --mode ev44 --instrument dummy
```

Run the monitor data histogramming and accumulation service:

```sh
python -m ess.livedata.services.monitor_data --instrument dummy
```

Run the monitors dashboard service:

```sh
# Development mode (runs on port 5007)
python -m ess.livedata.dashboard.monitors --instrument dummy

# Or production mode with gunicorn
LIVEDATA_INSTRUMENT=dummy gunicorn ess.livedata.dashboard.monitors_wsgi:application
```

Navigate to `http://localhost:5007` to see the dashboard.

## Running the services using Docker

Note: The docker is somewhat out of date and not all services are available currently.

You can also run all the services using Docker.
Use the provided `docker-compose-ess.livedata.yml` file to start Kafka:

```sh
LIVEDATA_INSTRUMENT=dummy docker-compose -f docker-compose-ess.livedata.yml up
```

This will start the Zookeeper, Kafka broker.
This can be then used with the services run manually as described above.

Alternatively, you can use profiles to start specific service groups:

```sh
# Start monitor services (includes fake data, monitor processing, and monitors dashboard)
LIVEDATA_INSTRUMENT=dummy docker-compose --profile monitor -f docker-compose.yml up

# Start detector services (includes fake data and detector processing)
LIVEDATA_INSTRUMENT=dummy docker-compose --profile detector -f docker-compose.yml up

# Start reduction dashboard
LIVEDATA_INSTRUMENT=dummy docker-compose --profile reduction -f docker-compose-ess.livedata.yml up
```

It will take a minute or two for the services to start fully.

When using the `monitor` profile, navigate to `http://localhost:5007` to see the monitors dashboard.
When using the `detector` profile, navigate to `http://localhost:5008` to see the detectors dashboard.
When using the `reduction` profile, navigate to `http://localhost:5009` to see the reduction dashboard.

Both dashboard profiles can be run simultaneously:

```sh
# Run both monitors and reduction dashboards
LIVEDATA_INSTRUMENT=dummy docker-compose --profile monitor --profile reduction -f docker-compose-ess.livedata.yml up
```

### Kafka Configuration

The services can be configured to connect to different Kafka brokers using environment variables.
There may be two distinct Kafka brokers: one upstream with raw data and one downstream for processed data and ESSlivedata control.

- `KAFKA_BOOTSTRAP_SERVERS`: Bootstrap servers for the upstream Kafka broker.
- `KAFKA_SECURITY_PROTOCOL`: Security protocol for the upstream Kafka broker.
- `KAFKA_SASL_MECHANISM`: SASL mechanism for the upstream Kafka broker.
- `KAFKA_SASL_USERNAME`: SASL username for the upstream Kafka broker.
- `KAFKA_SASL_PASSWORD`: SASL password for the upstream Kafka broker.

- `KAFKA2_BOOTSTRAP_SERVERS`: Bootstrap servers for the downstream Kafka broker.
- `KAFKA2_SECURITY_PROTOCOL`: Security protocol for the downstream Kafka broker.
- `KAFKA2_SASL_MECHANISM`: SASL mechanism for the downstream Kafka broker.
- `KAFKA2_SASL_USERNAME`: SASL username for the downstream Kafka broker.
- `KAFKA2_SASL_PASSWORD`: SASL password for the downstream Kafka broker.

Note that the security and authentication is not necessary when using the Kafka broker from the Docker container.
`KAFKA2_BOOTSTRAP_SERVERS` is also configured to default to use the Kafka broker from the Docker container.