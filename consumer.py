
from kafka import KafkaConsumer

import datetime

# Define Kafka consumer properties
bootstrap_servers = ['localhost:9092']
consumer = KafkaConsumer('krishna', bootstrap_servers=bootstrap_servers, auto_offset_reset='latest', enable_auto_commit=False)


def consume_messages_from_kafka_topics():
    while True:
        messages = consumer.poll(timeout_ms=1000)
        for topic_partition, messages in messages.items():
            for message in messages:
                # Process message received from Kafka
                with open('output.mp4', 'ab') as f:
                    f.write(message.value)
                    print(f"Received {len(message.value)} bytes at {datetime.datetime.now()}")
                print("\n",message,"\n")
            # Manually commit the offsets to mark the messages as processed
            consumer.commit()


# Call function to consume messages from Kafka topics
consume_messages_from_kafka_topics()

