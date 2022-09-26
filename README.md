# Evolving protobuf and consuming in Databricks DLT
The purpose of this prototype is to demonstrate how you can stream protobuf messages into Databricks DLT and handle evolution of the schema with the help of the Confluent schema registry.

# Prerequisites
The following Kafka and Schema Registry connection details (and credentials), should be saved as Databricks Secrets:
- SR_URL: Schema Registry URL (e.g. https://myschemaregistry.aws.confluent.cloud)
- SR_API_KEY: Schema Registry API Key 
- SR_API_SECRET: Schema Registry API Secret 
- KAFKA_KEY: Kafka API Key 
- KAFKA_SECRET: Kafka Secret 
- KAFKA_SERVER: Kafka host:port (e.g. mykafka.aws.confluent.cloud:9092)
- KAFKA_TOPIC: The Kafka Topic
- TARGET_SCHEMA: The target database where the streaming data will be appended into a Delta table (the destination table is named unified_gold)

Go here to learn how to save secrets: https://docs.databricks.com/security/secrets/index.html 

# Notebook overview
* The "Producer" notebook creates simulated messages and sends them to a Kafka topic. Use the widget values to increase the number of different schema versions used in the flow and the number of records sent per notebook run.
* The "DLT" notebook will be installed as a Continuous DLT Pipeline.
* The "Install_DLT_Pipeline" notebook will install the aforementioned DLT notebook.

# Instructions
1. Run the "Producer" notebook. This will publish some messages to Kafka. The notebook also installs a script in DBFS that will be used by the DLT notebook. That "init script" will ensure that the protobuf compiler (protoc) is installed on the cluster that runs the DLT pipeline.
2. Run the "Install_DLT_Pipeline" notebook.
3. Go back to the "Producer" notebook. Adjust the widget values (e.g. increase the number of versions). Ideally: you should be able to see the job automatically restart the pipeline. However, a bug currently prevents that automatic restart. To remediate this bug, you can have a separate job monitor the pipeline and restart it.

### Clean up
Stop and delete the job, pipeline and drop the target schema.