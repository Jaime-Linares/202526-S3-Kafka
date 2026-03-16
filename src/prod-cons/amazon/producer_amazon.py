import csv
import json
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9093'}
producer = Producer(conf)

topic = "transactions"

with open("data/amazon.csv", "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:
        message = json.dumps(row, ensure_ascii=False)
        producer.produce(topic, value=message)
        print("Sent:", message[:120], "...")

producer.flush()