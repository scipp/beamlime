# Configuring kafka environment variables
export KAFKA_SALS_USERNAME=DUMMY
export KAFKA_SALS_PASSWORD=DUMMY
export KAFKA_SECURITY_PROTOCOL=PLAINTEXT
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export LIVEDATA_INSTRUMENT=odin

# Start Kafka broker
docker-compose -f docker-compose.yml up
# Init Kafka - only if the `kafka_init` in the docker-compose.yml fails.
docker exec -it DOCKER_CONTAINER_ID sh /scripts/setup-kafka-topics.sh odin

# Fake Detector Data (Without file)
python -m ess.livedata.services.fake_detectors \
    --instrument ${LIVEDATA_INSTRUMENT:-dummy}
# Fake Detector Data (With file)
python -m ess.livedata.services.fake_detectors \
    --instrument ${LIVEDATA_INSTRUMENT:-dummy} \
    --nexus-file iron_simulation_sample_small.nxs  # From `ess.imaging.data`

# Detector Data Service
python -m ess.livedata.services.detector_data \
    --instrument ${LIVEDATA_INSTRUMENT:-dummy} \
    --dev

# Monitor Data Service
python -m ess.livedata.services.monitor_data \
    --instrument ${LIVEDATA_INSTRUMENT:-dummy} \
    --dev

# Dashboard
python -m pip install -e ".[dashboard]"
gunicorn ess.livedata.dashboard.reduction_wsgi:application -b 0.0.0.0:5007
