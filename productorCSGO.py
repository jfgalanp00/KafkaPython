#crear el productor
#    leer csv
#    generar el msn
#    mandar el msn topic (10s)

from kafka import KafkaProducer
from json import dumps
import time
import pandas as pd

producer = KafkaProducer(
    value_serializer = lambda m: dumps(m).encode('utf-8'),
    bootstrap_servers = ['localhost:9092'])

ruta_csv = '/home/iabd/Desktop/PruebasKafka/csgo_round_snapshots.csv'
kafka_topic = 'mapas_csgo'

def producir_mensaje():
    df = pd.read_csv(ruta_csv)

    for index, row in df.iterrows():
        message = {'map': row['map']}
        producer.send(kafka_topic, value=message)

#while True:
producir_mensaje()
time.sleep(10)
producer.close()
print("Final del dataset")