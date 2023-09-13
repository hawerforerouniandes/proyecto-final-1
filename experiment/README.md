# Introduction
Following this document you will be able to start up kafka infrastrctuture for locally purpose

## Prerequisites 
Before to continue with the tutorial please ensure you have installed the below tools
- Docker
## Kafka Environment configuration
Go to kafka folder and run below command through a terminal
```
docker-compose up -d 
```

To verify that it is up and running execute 
```
docker-compose ps
```

### Kafka topic
Now it is necessary to defined the next topics according to our needs

```
docker-compose exec kafka kafka-topics.sh --create --topic demo \
--partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
```

### Publishing and Consuming Messages
In this steps you will be able to emit an message and cosume it using kafka-client and kafka-consumer tools that are already installed into the kafka container 
Open a new terminal in the same place and execute next command
```
docker-compose exec kafka kafka-console-consumer.sh --topic demo \
--from-beginning --bootstrap-server kafka:9092
```

now go back to the first terminal , execute next command and after that emit type a meesage, to emit it press enter

```
docker-compose exec kafka kafka-console-producer.sh --topic demo \
  --broker-list kafka:9092
```

