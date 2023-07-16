# Running Batch processing from Kafka Topic
## Creating Topics from Kafka Shell
Then execute shell in container **kafka** using following script
```
docker exec -it kafka /bin/sh
```
All Kafka shell scripts are located in /opt/kafka_<version>/bin 
```
cd /opt/kafka_<version>/bin
```
after navigated into bin then create the topics using cli named it **messages**
```
kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 2 --topic messages
```

## Kafka Publisher
![alt text](https://raw.githubusercontent.com/muhk01/kafka_pyspark_batch_processing/main/images/1.PNG)
publish a message through 'messages' topic realtime.

## Kafka Consumer
![alt text](https://raw.githubusercontent.com/muhk01/kafka_pyspark_batch_processing/main/images/2.PNG)

Consume by batch for each minutes, using SparkStreaming processing into DataFrame, you could add more transformation, and other processing.
