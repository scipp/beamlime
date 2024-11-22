import json
import uuid

from confluent_kafka import Consumer


class ConfigSubscriber:
    def __init__(self, bootstrap_servers, service_name=None):
        # Generate unique group id using service name and random suffix, to ensure all
        # instances of the service get the same messages.
        group_id = f"{service_name or 'config-subscriber'}-{uuid.uuid4()}"

        self.config = {}
        self.consumer = Consumer(
            {
                'bootstrap.servers': bootstrap_servers,
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
            }
        )
        self.consumer.subscribe(['beamlime-control'])

    def start(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            key = msg.key().decode('utf-8')
            value = json.loads(msg.value().decode('utf-8'))
            self.config[key] = value
            self.on_config_update(key, value)

    def on_config_update(self, key, value):
        pass

    def get_config(self, key, default=None):
        return self.config.get(key, default)
