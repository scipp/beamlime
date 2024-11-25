import json
import uuid

from confluent_kafka import Consumer


class ConfigSubscriber:
    def __init__(self, bootstrap_servers, service_name=None):
        # Generate unique group id using service name and random suffix, to ensure all
        # instances of the service get the same messages.
        group_id = f"{service_name or 'config-subscriber'}-{uuid.uuid4()}"
        self._consumer = Consumer(
            {
                'bootstrap.servers': bootstrap_servers,
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
            }
        )
        self._config = {}
        self._running = False

    def get_config(self, key, default=None):
        return self._config.get(key, default)

    def start(self):
        self._running = True
        self._consumer.subscribe(['beamlime.control'])
        try:
            while self._running:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    print(f'Consumer error: {msg.error()}')
                    continue

                key = msg.key().decode('utf-8')
                value = json.loads(msg.value().decode('utf-8'))
                print(f'Updating config: {key} = {value} at {msg.timestamp()}')
                self._config[key] = value
        except KeyboardInterrupt:
            pass
        finally:
            self._running = False
            self._consumer.close()

    def stop(self):
        self._running = False
