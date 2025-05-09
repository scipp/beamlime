# Services

## Overview

### Main services

The following main services are available:

```sh
python -m beamlime.services.monitor_data --instrument dummy
python -m beamlime.services.detector_data --instrument dummy
python -m beamlime.services.data_reduction --instrument dummy
python -m beamlime.services.timeseries --instrument dummy
```

For testing, each of these should be run with the `--dev` argument.
This will run the services with a simplified topic structure and make them compatible with the fake data services [below](#fake-data-services).

Note also the `--sink png` argument, which will save the outputs as PNG files instead of publishing them to Kafka.
This allows for testing the service outputs without running the dashboard.

### Dashboard

The dashboard service can be run in development mode using

```sh
python -m beamlime.services.dashboard --instrument dummy
```

The dashboard service can be run in production mode using

```sh
BEAMLIME_INSTRUMENT=dummy gunicorn beamlime.services.wsgi:application
```

### Fake data services

The following fake data services are available:

```sh
python -m beamlime.services.fake_monitors --mode ev44 --instrument dummy
python -m beamlime.services.fake_detectors --instrument dummy
python -m beamlime.services.fake_logdata --instrument dummy
```

## Example: Running the monitor data service

Services can be found in `beamlime.services`.
Configuration is in `beamlime.config.defaults`.
By default the files with the `dev` suffix are used.
Set `BEAMLIME_ENV` to, e.g., `staging` to use the `staging` configuration.

For a local demo, run the fake monitor data producer:

```sh
python -m beamlime.services.fake_monitors --mode ev44 --instrument dummy
```

Run the monitor data histogramming and accumulation service:

```sh
python -m beamlime.services.monitor_data --instrument dummy
```

Run the dashboard service:

```sh
BEAMLIME_INSTRUMENT=dummy gunicorn beamlime.services.wsgi:application
```

Navigate to `http://localhost:8000` to see the dashboard.


## Running the services using Docker

Note: The docker is somewhat out of date and not all services are available currently.

You can also run all the services using Docker.
Use the provided `docker-compose-beamlime.yml` file to start Kafka:

```sh
BEAMLIME_INSTRUMENT=dummy docker-compose -f docker-compose-beamlime.yml up
```

This will start the Zookeeper, Kafka broker.
This can be then used with the services run manually as described above.

Alternatively, the `monitor` or `detector` profile can be used to start the respective services in Docker:

```sh
BEAMLIME_INSTRUMENT=dummy docker-compose --profile monitor -f docker-compose-beamlime.yml up
```

It will take a minute or two for the services to start fully.
Navigate to `http://localhost:8000` to see the dashboard.

### Kafka Configuration

The services can be configured to connect to different Kafka brokers using environment variables. There may be two distinct Kafka brokers: one upstream with raw data and one downstream for processed data and Beamlime control.

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