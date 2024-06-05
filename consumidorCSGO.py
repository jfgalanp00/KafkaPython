from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'mapas_csgo',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda m: loads(m.decode('utf-8')),
    bootstrap_servers=['localhost:9092'])

for m in consumer:
    print(m.value)