## Steps for running Kafka connect and creating Debezium PostgresSQL connector

1. Login to VM using SSH

```
	ssh cslabsuser@az-cslabs-vm1-sdfsdfjl.eastus.cloudapp.azure.com

```

2. Acquire the ownership of the Kafka connect config file using the following command.

```
	sudo chown $USER /usr/lib/kafka_2.12-3.3.1/config/connect-distributed.properties

```

3. Open VS Code in the windows VM and connect to the linux VM using **Remote-SSH: Connect to Host..** and the same user name as step 1. 

4. Open the config file `/usr/lib/kafka_2.12-3.3.1/config/connect-distributed.properties` using **File:Open** and Update event hub namespace and connection string and save.

5. Using the ssh terminal Run Kafa connect as follows. 
```
	/usr/lib/kafka_2.12-3.3.1/bin/connect-distributed.sh /usr/lib/kafka_2.12-3.3.1/config/connect-distributed.properties  

```
6. Use another ssh terminal session. Create Debezium postgres SQL connection by running following command. 

```
	curl -X POST -H "Content-Type: application/json" --data @/usr/lib/kafka_2.12-3.3.1/config/pg-source-connector.json http://	localhost:8083/connectors

```

7. Check status of Debezium postgres SQL connection. You will see "Running" status. 

```
	curl -s http://localhost:8083/connectors/retail-connector/status

```

8. Switch to terminal 1 and you will see Kafa connect sending the change streams to the event hubs.