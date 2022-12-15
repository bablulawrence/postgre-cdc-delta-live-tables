## Steps for running Kafka connect and creating Debezium PostgresSQL connector

1. Login to VM using SSH

```
	ssh cslabsuser@az-cslabs-vm1-sdfsdfjl.eastus.cloudapp.azure.com
```

2. Update event hub namespace and connection string Kafa connect config file. 
```
	sudo nano /usr/lib/kafka_2.12-3.3.1/config/connect-distributed.properties

```
3. Run Kafa connect
```
	/usr/lib/kafka_2.12-3.3.1/bin/connect-distributed.sh /usr/lib/kafka_2.12-3.3.1/config/connect-distributed.properties  

```
4. Create Debezium postgres SQL connection 

```
	curl -X POST -H "Content-Type: application/json" --data @/usr/lib/kafka_2.12-3.3.1/config/pg-source-connector.json http://	localhost:8083/connectors

```

5. Check status of Debezium postgres SQL connection 

```
	curl -s http://localhost:8083/connectors/retail-connector/status

```