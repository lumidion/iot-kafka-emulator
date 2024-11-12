# IoT Kafka Emulator

The IoT Kafka Emulator exists to generate large amounts of IoT data and dump it into Kafka. The emulator is very fast
and, depending on configuration, can write 1 million records to Kafka per second

## Getting Started

1. Set up your Kafka cluster, if it does not already exist

2. Create the following docker compose file and add in the relevant information for the Kafka cluster that you already
   created

    ```yaml
    version: '3.8'
    
    services:
      emulator:
        image: 'lumidion/iot-kafka-emulator:latest'
        environment:
          KAFKA_BATCH_INTERVAL: "500"
          KAFKA_TOPIC_NAME: "<sample_topic_name>"
          KAFKA_BROKER_URL: "<host>:<port>"
          KAFKA_BATCH_SIZE: "10000"
          THREAD_NUMBER: "500"
    ```

3. Run `docker compose up` to start the service and see sample data flowing into your Kafka topic immediately