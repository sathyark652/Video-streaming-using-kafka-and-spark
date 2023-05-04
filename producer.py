
import argparse
import os
import time
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description='Send video to Kafka topic')
parser.add_argument('filepath', type=str, help='Path of input video file')
parser.add_argument('topic_name', type=str, help='Name of Kafka topic to send video to')
args = parser.parse_args()


# Define Kafka producer properties
bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_messages_to_kafka_topics():
    try:
        with open(args.filepath, 'rb') as f:
            video = f.read()

        # Determine optimal chunk size based on file size
        filesize = os.path.getsize(args.filepath)
        chunk_size = max(filesize // 100, 1000000)

        # Send video to Kafka topic in chunks with time sent
        for i in range(0, len(video), chunk_size):
            chunk = video[i:i+chunk_size]
            timestamp = time.time()
            producer.send(args.topic_name, value=chunk, timestamp_ms=int(timestamp*1000))

        producer.flush()
        print("All video chunks sent to Kafka topic")
    except Exception as e:
        print(f"Error sending video to Kafka: {e}")


# Call function to send messages to Kafka topics
send_messages_to_kafka_topics()



