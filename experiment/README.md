# Introduction
Following this document you will be able to start up kafka infrastrctuture for locally purpose

## Prerequisites 
Before to continue with the tutorial please ensure you have installed the below tools
- docker
- docker-compose
## Kafka Environment configuration

Execute the below commands to download and install kafka cliente 

```
chmod +x kafka-installer.sh
sudo ./kafka-installer.sh
```

it will downaload the kafka-client and put it down /opt folder you will use it more later

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
/opt/kafka-client/bin/kafka-topics.sh --create \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--config retention.ms=-1 \
--topic questions_processor
```

### Publishing and Consuming Messages
In this steps you will be able to emit an message and cosume it using kafka-client and kafka-consumer tools that are already installed into the kafka container 
Open a new terminal in the same place and execute next command
```
/opt/kafka-client/bin/kafka-console-consumer.sh --topic questions_processor \
--bootstrap-server localhost:9092
```

now go back to the first terminal , execute next command and after that emit type a meesage, to emit it press enter

```
/opt/kafka-client/bin/kafka-console-consumer.sh --topic questions_processor \
--bootstrap-server localhost:9092
```

