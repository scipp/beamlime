# DREAM demo

## Setup

### Packages

```sh
python -m venv dream-demo
source dream-demo/bin/activate
pip install "git+https://github.com/scipp/beamlime.git@dream-v2#egg=beamlime[dream,dashboard]"
```

### Data

```sh
scp login.esss.dk:'/dmsc/scipp/dream/268227_00024779_*' .
```

## Running

### Docker

Either clone the repository or download [docker-compose-beamlime.yml](https://raw.githubusercontent.com/scipp/beamlime/refs/heads/dream-v2/docker-compose-beamlime.yml).
I usually run the Docker containers in a detached screen session, so I can reattach later if needed, but this is optional.

```sh
screen -S kafka
BEAMLIME_INSTRUMENT=dream docker-compose -f docker-compose-beamlime.yml up
```

Press `Ctrl + A` then `D` to detach from the screen session.
You can later reattach to the session with:

```sh
screen -r kafka
```

### Beamlime

In individual terminal windows, run (make sure to `source dream-demo/bin/activate` in each):

```sh
python -m beamlime.services.fake_detectors --instrument dream --nexus-file 268227_00024779_Si_BC_offset_240_deg_wlgth.hdf
python -m beamlime.services.data_reduction --instrument=dream --dev
python -m beamlime.dashboard.reduction --instrument=dream
```

## Usage

### Connecting

Navigate to `http://localhost:5009`.
IDEs such as VScode with remote extensions will automatically forward this port to the host machine if you click on the link in the terminal when starting the dashboard.

### Data reduction

Try to configure and start workflows.
It usually takes a couple of seconds since static parts of the workflow (such as instrument geometry from NeXus files) have to be computed initially.

The data rate is set to something very low, so you can actually see changes in the plots.
The fake data source loops over the same events from the data file indefinitely.
We can easily deal with much higher rates.

Some config errors are caught and displayed in the UI, but if the backend reduction workflow fails you will not see an error in the UI.
You can check the logs in the terminal where you started the `data_reduction` service.

The streamed data is using choppers settings with the `high_flux_BC240` settings.
You can see/demonstrate artifacts from (incorrect) WFM frame merging by selecting `high_flux_BC215`.

#### Known issues

- The `logarithmic` bin settings work in the backend, but the UI plotting cannot deal with it.
- Changing units of edges probably does not work everywhere either.
- There is no indication of stopped workflows in the plots.
  Click "Remove" to remove a plot item.

## Troubleshooting

Beamlime persists some config in Kafka.
In case this is causing hard-to-debug issues, you can clear the Kafka topics.
First, cancel all the services, otherwise some topics are automatically recreated (with incorrect settings).
Then run:

```sh
docker exec -it kafka-broker sh /scripts/setup-kafka-topics.sh dream
```

Alternatively, just `screen -r kafka`, `Ctrl + C` the entire thing, then relaunch.

You will most likely have to restart the services as they are not designed to handle topic removal gracefully.