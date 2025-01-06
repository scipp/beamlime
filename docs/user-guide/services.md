# Services

## Running the monitor data service

Services can be found in `beamlime.services`.
Configuration is in `beamlime.config.defaults`.
By default the files with the `dev` suffix are used.
Set `BEAMLIME_ENV` to, e.g., `staging` to use the `staging` configuration.

For a local demo, run the fake producer:

```sh
python -m beamlime.services.fake_producer --mode ev44 --instrument dummy
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

You can also run all the services using Docker.
Use the provided `docker-compose-beamlime.yml` file to start the services:

```sh
BEAMLIME_INSTRUMENT=dummy docker-compose -f docker-compose-beamlime.yml up
```

This will start the Zookeeper, Kafka broker, and all the necessary services for the monitor data dashboard.
It will take a minute or two for the services to start fully.
Navigate to `http://localhost:8000` to see the dashboard.
