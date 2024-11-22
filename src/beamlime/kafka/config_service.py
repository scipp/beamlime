import json

from confluent_kafka import Consumer, KafkaError, Producer


class ConfigService:
    """
    Service for managing configuration updates via Kafka.

    The service listens for updates on the 'beamlime-control' topic and updates
    the local configuration accordingly. It also provides methods for updating
    the configuration and retrieving the current configuration.

    The topic for this service should be created as compacted:

    .. code-block:: bash
        kafka-topics.sh --create --bootstrap-server localhost:9092 \
        --topic beamlime-control --config cleanup.policy=compact \
        --config min.cleanable.dirty.ratio=0.01 \
        --config segment.ms=100
    """

    def __init__(self, bootstrap_servers):
        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
        }
        consumer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'control-service',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        }

        self._producer = Producer(producer_conf)
        self._consumer = Consumer(consumer_conf)
        self._config = {}
        self._running = False
        self._local_updates = set()  # Track locally initiated updates

    def delivery_callback(self, err, msg):
        if err:
            print(f'Message delivery failed: {err}')

    def update_config(self, key, value):
        try:
            # Mark this as a local update
            update_id = f"{key}:{hash(str(value))}"
            self._local_updates.add(update_id)

            self._producer.produce(
                'beamlime-control',
                key=str(key).encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_callback,
            )
            self._producer.flush()
            self._config[key] = value
        except Exception as e:
            self._local_updates.discard(update_id)
            print(f'Failed to update config: {e}')

    def get_config(self, key, default=None):
        return self._config.get(key, default)

    def start(self):
        self._running = True
        self._consumer.subscribe(['beamlime-control'])
        try:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f'Consumer error: {msg.error()}')
                        break

                try:
                    key = msg.key().decode('utf-8')
                    value = json.loads(msg.value().decode('utf-8'))

                    # Only update if not from our own producer
                    update_id = f"{key}:{hash(str(value))}"
                    if update_id not in self._local_updates:
                        self._config[key] = value
                    else:
                        self._local_updates.discard(update_id)
                except Exception as e:
                    print(f'Failed to process message: {e}')

        except KeyboardInterrupt:
            pass
        finally:
            self._running = False
            self._consumer.close()

    def stop(self):
        self._running = False
