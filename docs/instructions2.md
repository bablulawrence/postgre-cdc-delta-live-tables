# Deploying PostgreSQL CDC with Azure Event Hub

## Provision a Azure VM

Provision windows VM using Windows Server 2022 Datacenter Azure Edition

## Install WSL2 on windows server

Run following command as a powershell administrator to install WSL2 and Ubuntu on the Azure VM
	
`wsl --install`

Please note this will work on Windows Server 2022

## Create Event hub

Follow the instructions [here](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-create) to create an event hub namespace and an event hub 

## Install PostgreSQL

Follow the instruction in the [blog]((https://chloesun.medium.com/set-up-postgresql-on-wsl2-and-connect-to-postgresql-with-pgadmin-on-windows-ca7f0b7f38ab)
) to setup WSL2 on windows

## Import Retail Org database backup to PostgreSQL

1. Download the /postgres/backups/pg_dump_retail_org_sql file to the Azure VM. 
2. Create a new database for Retail Org.

	`CREATE DATABASE retail_org`

3. Import the backup file into above database 

	`psql retail_org1 < retail_org.sql`

## Install Kafa Connect

1. First install all dependencies

```sh
	sudo apt update
	sudo apt install default-jre
	sudo apt install openjdk-8-jre-headless
```
2. Go to [Apache Kafka Downloads](https://michaeljohnpena.com/blog/kafka-wsl2/) to check for the latest release. Right click on the Binary downloads like and copy the link. Download the tgz file and extract it

```sh
	wget  wget https://downloads.apache.org/kafka/3.3.1/kafka_2.12-3.3.1.tgz
	tar -xvzf kafka_2.13-3.2.0.tgz
```
3. Add following lines to .bashrc file

```	export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
	export KAFKA_HOME=/usr/lib/kafka_2.12-3.3.1
```
4. Run `source ~/.bashrc` to export the new environment variables without opening WSL. 

5. Start Zookeeper buy running command - `$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties`.
6. Start Kafka by running command - `$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties`. 
7. For testing(Optional) use producer to publish events using command - `$KAFKA_HOME/bin/kafka-console-producer.sh --topic sample-topic --broker-list localhost:9092`
8. For testing(Optional) use consumer to receive events using command - `$KAFKA_HOME/bin/kafka-console-consumer.sh --topic sample-topic --from-beginning --bootstrap-server localhost:9092`
10. Type something in publisher tab and you will see it reflected in consumer tab. 

## Install Debizium connector for PostgreSQL

1. Go to [Debezium PostgreSQL deploying connector page](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-deploying-a-connector). Copy the link of `connectors plug-in archive`

2. Download the plugin and extract the contents

```sh
	wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/1.2.5.Final/debezium-connector-postgres-1.2.5.Final-plugin.tar.gz
	tar -xvzf debezium-connector-postgres-1.2.5.Final-plugin.tar.gz

```
3. Copy debezium JAR files to Kafka installation. 

```sh
	cp debezium-connector-postgres/*.jar $KAFKA_HOME/libs
```
4. Update the content of Kafka connect properties file in `$KAFKA_HOME/config/connect-distributed.properties` as shown below

```sh
	bootstrap.servers={event hub namespace}.servicebus.windows.net:9093
	group.id=connect-cluster-group

	# connect internal topic names, auto-created if not exists
	config.storage.topic=connect-cluster-configs
	offset.storage.topic=connect-cluster-offsets
	status.storage.topic=connect-cluster-status

	# internal topic replication factors - auto 3x replication in Azure Storage
	config.storage.replication.factor=1
	offset.storage.replication.factor=1
	status.storage.replication.factor=1

	rest.advertised.host.name=connect
	offset.flush.interval.ms=10000

	key.converter=org.apache.kafka.connect.json.JsonConverter
	value.converter=org.apache.kafka.connect.json.JsonConverter
	internal.key.converter=org.apache.kafka.connect.json.JsonConverter
	internal.value.converter=org.apache.kafka.connect.json.JsonConverter

	internal.key.converter.schemas.enable=false
	internal.value.converter.schemas.enable=false

	# required EH Kafka security settings
	security.protocol=SASL_SSL
	sasl.mechanism=PLAIN
	sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{Event hub connection string}";

	producer.security.protocol=SASL_SSL
	producer.sasl.mechanism=PLAIN
	producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{Event hub connection string}";

	consumer.security.protocol=SASL_SSL
	consumer.sasl.mechanism=PLAIN
	consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{Event hub connection string}";

	plugin.path=/usr/lib/kafka_2.12-3.3.1/libs # path to the libs directory within the Kafka release

```
5. Start Kafka connect cluster by running following command 

```
$KAFKA_HOME/bin/connect-distributed.sh connect.properties

```
6. Wait for few mins and check whether Kafka Connect internal topics in Azure Event Hubs.

## Change PostgreSQL replication to Logical

To perform logical replication in PostgreSQL, you’ve to change the wal_level = logical parameter. To change the value of this parameter, you have to open the `postgresql.conf` file. This file can be found in the following directory:

` /etc/postgresql/{version}/main e.g. /etc/postgresql/12/main`

Open the file, go to the WRITE-AHEAD LOG settings. Uncomment the parameter wal_level and set it to the following:

` wal_level = logical `

Restart the PostgreSQL service using command ` sudo service postgresql restart `

Note the you should not change the owner of the `postgresql.conf` file. If you get an error while restrating the PostgreSQL service please change the owner back to the one before by running following command. 

`chown postgres sample2`

## Install Wal2Json plugin

Install wall2json plugin using the following command. 

`sudo apt-get install postgresql-12-wal2json`

## Create Debezium PostgreSQL source connection

1. In the current folder create pg-source-connector.json file with the details for the Azure PostgreSQL instance

```json
	{
		"name": "retail-org-connector",
		"config": {
			"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
			"database.hostname": "127.0.0.1",
			"database.port": "5432",
			"database.user": "postgres",
			"database.password": "{password}}",
			"database.dbname": "retail_org1",
			"database.server.name": "retail-server",
			"plugin.name": "wal2json",
			"table.whitelist": "public.sales_orders,public.customers,public.products"
		}
	}

```
2. Create the connector by running following command. 

`curl -X POST -H "Content-Type: application/json" --data @pg-source-connector.json http://localhost:8083/connectors`

## Run Kcat to consume events

1. Create a kcat(kafkacat) config file `kcat.conf` in current directory with following co

```sh
	metadata.broker.list={event hub namespace}.servicebus.windows.net:9093
	security.protocol=SASL_SSL
	sasl.mechanisms=PLAIN
	sasl.username=$ConnectionString
	sasl.password=Endpoint=sb://az-cslabs-phase2-ehn1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={shared access key}
```
2. Run following command to list all topics

`kafkacat -b az-cslabs-phase2-ehn1.servicebus.windows.net:9093 -L`

3. Run following command to list all topics
`kafkacat -b az-cslabs-phase2-ehn1.servicebus.windows.net:9093 -t retail-server.public.customers -o beginning -F ~/.config/kafkacat.conf`

## Perform instert to Retail Org database

Insert rows to customer table using following statement 

```sql

INSERT INTO customers (customer_id,tax_id,tax_code,customer_name,state,city,postcode,street,number,unit,region,district,lon,lat,
			ship_to_address,valid_from,valid_to,units_purchased,loyalty_segment)
VALUES (100,340758025.0,'A','Bablu','MI','WYANDOTTE','48192','CORA',1574.0,'Test','MI','test',
	-83.1629844,42.2175195,'MI', 1574.0,1534928698,2.0,0);

```

# References

[Setup Postgrest On WSL2](https://chloesun.medium.com/set-up-postgresql-on-wsl2-and-connect-to-postgresql-with-pgadmin-on-windows-ca7f0b7f38ab)

[Install Kafka On WSL2](https://michaeljohnpena.com/blog/kafka-wsl2/)

[Create event hub](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-create)

[Download and install Debizium connector](https://dev.to/azure/tutorial-set-up-a-change-data-capture-architecture-on-azure-using-debezium-postgres-and-kafka-49h6)

[Download and install Debizium connector MS](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-kafka-connect-debezium)

[Change Postgres replication to logical](https://hevodata.com/learn/postgresql-logical-replication/#:~:text=To%20perform%20logical%20replication%20in,conf%20file.)

[Install Debezium PostgreSQL source connector](https://dev.to/azure/tutorial-set-up-a-change-data-capture-architecture-on-azure-using-debezium-postgres-and-kafka-49h6)

[Run kafkacat to read events](https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/quickstart/kafkacat)
