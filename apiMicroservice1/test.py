import os
import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

class KeyClass:
    def __init__(self, identifier):
        self.identifier = identifier

class ValueClass:
    def __init__(self, identifier, status, ts, message):
        self.identifier = identifier
        self.status = status
        self.ts = ts
        self.message = message

def key_to_dict(key_obj, ctx):
    return {"identifier": key_obj.identifier}

def value_to_dict(value_obj, ctx):
    return {
        "identifier": value_obj.identifier,
        "status": value_obj.status,
        "ts": value_obj.ts,
        "message": value_obj.message
    }

class RideAvroProducer:
    def __init__(self, props):
        key_schema_str = self.load_schema(props['schema.key'])
        value_schema_str = self.load_schema(props['schema.value'])

        schema_registry_props = {'url': props['schema_registry.url']}
        schema_registry_client = SchemaRegistryClient(schema_registry_props)

        self.key_serializer = AvroSerializer(schema_registry_client, key_schema_str, to_dict=key_to_dict)
        self.value_serializer = AvroSerializer(schema_registry_client, value_schema_str, to_dict=value_to_dict)

        producer_props = {'bootstrap.servers': props['bootstrap.servers']}
        self.producer = Producer(producer_props)

    @staticmethod
    def load_schema(schema_path):
        with open(schema_path) as f:
            schema_str = f.read()
        return schema_str

    def produce(self, topic, key, value):
        try:
            serialized_key = self.key_serializer(key, SerializationContext(topic, MessageField.KEY))
            serialized_value = self.value_serializer(value, SerializationContext(topic, MessageField.VALUE))
            self.producer.produce(topic, key=serialized_key, value=serialized_value, on_delivery=self.delivery_report)
            self.producer.flush()
        except ValueError as e:
            print(f"🚫 Schema Validation Error: The provided data does not conform to the expected data contract. {e}")

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f"🚨 Delivery Failed: {msg.key()}: {err}")
            return
        print(f'✅ Record {msg.key()} Successfully Produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

props = {
    'schema.key': 'status_identifier.avsc',
    'schema.value': 'status_value.avsc',
    'schema_registry.url': 'http://localhost:8081',
    'bootstrap.servers': 'localhost:9092'
}

producer = RideAvroProducer(props)

key = KeyClass(identifier="L03-S01-05:10")
value = ValueClass(
    identifier="L03-S01-05:10",
    status="Open",
    ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
    message="Signalisation"
)

producer.produce('status', key, value)
