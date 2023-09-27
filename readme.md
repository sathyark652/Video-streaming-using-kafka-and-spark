# Video Streaming with Kafka, Spark, and MongoDB
This project demonstrates a video streaming pipeline using Apache Kafka, Apache Spark, and MongoDB. The primary objective is to process video frames one by one, perform various manipulations using Spark, and store the results in MongoDB. Below are the steps to set up and run this project:

Setup Kafka and Zookeeper
Start Kafka and Zookeeper services to establish the messaging infrastructure.

### Start Zookeeper
sudo systemctl start zookeeper

### Start Kafka
sudo systemctl start kafka

### Create Kafka Topic
Create a Kafka topic named "krishna" to serve as the message channel for video frames.

kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic krishna

### Run Consumer
Run the consumer.py script to consume video frames from the Kafka topic. This script is responsible for receiving video frames and passing them to Spark for processing.

python consumer.py

### Run Producer
Next, run the producer.py script to send video frames to the Kafka topic. You can configure this script to provide the video source

python producer.py


Run Spark Streaming
Execute the streaming.py script to process video frames using Spark Streaming. This script should carry out manipulations or analysis on the video frames as required.

python streaming.py
Store Video Frame Details
The results of the Spark streaming operations, such as frame manipulations, will be stored in MongoDB for further analysis or retrieval.
Note: Ensure that you have Kafka, Spark, and MongoDB properly configured on your system, and you have installed the necessary Python libraries for Kafka, Spark, and MongoDB integration.

With this setup, you can experiment with real-time video frame processing and explore the capabilities of Kafka, Spark, and MongoDB in a streaming data context. This project serves as an educational resource for understanding video streaming architectures and distributed data processing.
