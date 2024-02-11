import os
import datetime
import mysql.connector
from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pydantic import BaseModel

app = FastAPI()

base_path = os.path.dirname(os.path.abspath(__file__))

class StatusModel(BaseModel):
    identifier: str
    status: str
    message: str

class LineUpdateModel(BaseModel):
    identifier: str
    delay_minutes: int
    message: str

db_config = {
    "host": "localhost",
    "port": "3306",
    "database": "metro",
    "user": "root",
    "password": "123456"
}


def check_identifier_in_db(identifier):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM train_passages WHERE passage_timeid = '{identifier}'")
        (count,) = cursor.fetchone()
        return count > 0
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

class KeyClass:
    def __init__(self, identifier):
        self.identifier = identifier

class ValueClass:
    def __init__(self, identifier, status, ts, message, delay_minutes=None):
        self.identifier = identifier
        self.status = status
        self.ts = ts
        self.message = message
        self.delay_minutes = delay_minutes


def key_to_dict(key_obj, ctx):
    return {"identifier": key_obj.identifier}

def value_to_dict(value_obj, ctx):
    return {
        "identifier": value_obj.identifier,
        "status": value_obj.status,
        "delay_minutes": value_obj.delay_minutes,
        "ts": value_obj.ts,
        "message": value_obj.message
    }


class MetroAvroProducer:
    def __init__(self, key_schema_path, value_schema_path, schema_registry_url, bootstrap_servers):
        key_schema_str = self.load_schema(key_schema_path)
        value_schema_str = self.load_schema(value_schema_path)
        schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
        self.key_serializer = AvroSerializer(schema_registry_client, key_schema_str, to_dict=key_to_dict)
        self.value_serializer = AvroSerializer(schema_registry_client, value_schema_str, to_dict=value_to_dict)
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

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
            raise HTTPException(status_code=400, detail=f"🚫 Schema Validation Error: The provided data does not conform to the expected data contract.: {e}")

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f"🚨 Delivery Failed: {msg.key()}: {err}")
            return
        print(f'✅ Record {msg.key()} Successfully Produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

producer_status = MetroAvroProducer(
    key_schema_path=os.path.join(base_path, 'schemas', 'status_identifier.avsc'),
    value_schema_path=os.path.join(base_path, 'schemas', 'status_value.avsc'),
    schema_registry_url='http://localhost:8081',
    bootstrap_servers='localhost:9092'
)

producer_line_update = MetroAvroProducer(
    key_schema_path=os.path.join(base_path, 'schemas', 'line_update_identifier.avsc'),
    value_schema_path=os.path.join(base_path, 'schemas', 'line_update_value.avsc'),
    schema_registry_url='http://localhost:8081',
    bootstrap_servers='localhost:9092'
)

@app.post("/send-status/")
def send_status(status_data: StatusModel):
    if not check_identifier_in_db(status_data.identifier):
        raise HTTPException(status_code=404, detail="Identifier not found in database")

    key = KeyClass(identifier=status_data.identifier)
    value = ValueClass(
        identifier=status_data.identifier,
        status=status_data.status,
        ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        message=status_data.message
    )

    try:
        producer_status.produce('status', key, value)
    except HTTPException as e:
        raise e
    return {"message": "Data sent to Kafka successfully"}

@app.post("/update-line/")
def update_line(line_update: LineUpdateModel):
    if not check_identifier_in_db(line_update.identifier):
        raise HTTPException(status_code=404, detail="Identifier not found in database")

    key = KeyClass(identifier=line_update.identifier)
    value = ValueClass(
        identifier=line_update.identifier,
        status="Update",
        delay_minutes=line_update.delay_minutes,  
        ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        message=line_update.message
    )

    try:
        producer_line_update.produce('update_line', key, value)
    except HTTPException as e:
        raise e
    return {"message": "Line update sent to Kafka successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)