# Databricks notebook source
# MAGIC %md
# MAGIC # Beginner demo
# MAGIC ## Reading and writing protobuf with PySpark
# MAGIC This is a simple demo to show how protobuf can be serialized and deserialized. Run each cell one-by-one and read the commentary to understand any (expected) errors.
# MAGIC
# MAGIC ## Prerequisite
# MAGIC You must have access to a Confluent Kafka cluster. You will need the required connection details to run this demo. Important: make sure you protect secrets and credentials using Databricks secrets!

# COMMAND ----------

# DBTITLE 1,Faker is used to generate some fake data
# MAGIC %pip install Faker

# COMMAND ----------

from faker import Faker
fake_generator = Faker()

my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

# COMMAND ----------

# DBTITLE 0,Create some fake data using Faker
df_fake_data = spark.range(10000)
df_fake_data = df_fake_data.withColumn("name", F.udf(fake_generator.name)())
df_fake_data = df_fake_data.withColumn("address", F.udf(fake_generator.address)())
df_fake_data = df_fake_data.withColumn("uuid", F.udf(fake_generator.uuid4)())
df_fake_data = df_fake_data.selectExpr("struct(*) as event")

# COMMAND ----------

# DBTITLE 1,Notice the new package pyspark.sql.protobuf.functions
import pyspark.sql.functions as F
import pyspark.sql.protobuf.functions as PF

# COMMAND ----------

# DBTITLE 1,The data intended to be serialized as protobuf should be organized in a struct
display(df_fake_data)

# COMMAND ----------

# DBTITLE 1,Let's try to serialize using the protobuf serialization function: to_protobuf
df_fake_data = df_fake_data.withColumn("proto_payload", PF.to_protobuf(F.col("event")))

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Why did that fail?
# MAGIC Consider the fact that, unlike Avro, protobuf messages themselves do not convey schema details. The binary payload uses positional logic to store the various fields values. Without the schema information, the reader/deserializer has no way to know what the fields are or where their position is within the binary payload
# MAGIC
# MAGIC ## Let's review the error
# MAGIC ```Required configuration schema.registry.subject is missing in options.```
# MAGIC
# MAGIC When we look back at the code, we were missing an essential input: `options`:
# MAGIC ```df_fake_data = df_fake_data.withColumn("proto_payload", PF.to_protobuf(F.col("events")))``` 
# MAGIC
# MAGIC ## Why are options necessary?
# MAGIC Remember: the schema is not in the payload. The protobuf functions need some mechanism for retrieving the schema of the message(s).
# MAGIC
# MAGIC ## What options are available?
# MAGIC When using protobuf functions, you have these options:
# MAGIC 1. Use a descriptor file
# MAGIC 2. Use Confluent Schema Registry (a Databricks-exclusive feature)

# COMMAND ----------

SR_URL = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_URL")
SR_API_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_KEY")
SR_API_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_SECRET")

# COMMAND ----------

schema_registry_conf = {
  # Authentication details:
  "confluent.schema.registry.basic.auth.credentials.source" : "USER_INFO",
  "confluent.schema.registry.basic.auth.user.info" : f"{SR_API_KEY}:{SR_API_SECRET}",
  # End-point details:
  "schema.registry.subject" : "app-events-value",
  "schema.registry.address" : f"{SR_URL}",
}

# COMMAND ----------

df_fake_data = df_fake_data.withColumn("proto_payload", PF.to_protobuf(F.col("event"), options = schema_registry_conf))

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC .
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Why did that fail?
# MAGIC The `to_protobuf` function does not register the schema with the Schema Registry. 
# MAGIC
# MAGIC ## Let's review the error
# MAGIC ```
# MAGIC Schema from schema registry could not be initialized. Error while fetching schema for subject 'app-events-value' from the registry: Subject 'app-events-value' not found.; error code: 40401.
# MAGIC ```
# MAGIC
# MAGIC ## The schema has to exist first!
# MAGIC To solve the error, the schema needs to first be registered with the Schema Registry. 
# MAGIC
# MAGIC ## What is `subject 'app-events-value'`?
# MAGIC The TOPIC we used is `app-events`, so what is `app-events-value` all about? The answer lies in the fact that, in Kafaka, each message is composed of a VALUE and a KEY. Thus, a message's key can have its own schema registered in the Schema Registry. For our needs, we will only have to worry about the `app-events-value` schema.

# COMMAND ----------

# DBTITLE 1,Install Confluent's library
# MAGIC %pip install --upgrade confluent_kafka

# COMMAND ----------

KAFKA_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")

# COMMAND ----------

config = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": f"{KAFKA_KEY}",
  "sasl.password": f"{KAFKA_SECRET}",
  "session.timeout.ms": "45000"
}  

# COMMAND ----------

from confluent_kafka.admin import AdminClient

# COMMAND ----------

# DBTITLE 1,Protobuf schema definition
protodef = """
syntax = "proto3";

package demo;

message event {
  optional int64 id = 1;
  optional string name = 2;
  optional string address = 3;
  optional string uuid = 4;
}
"""

# COMMAND ----------

schema_registry_conf_confluent = {
  'url': SR_URL,
  'basic.auth.user.info': f'{SR_API_KEY}:{SR_API_SECRET}'
}

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

# COMMAND ----------

schema_registry_client = SchemaRegistryClient(schema_registry_conf_confluent)

# COMMAND ----------

# DBTITLE 1,Register the schema with Confluent Schema Registr
p_schema = Schema(protodef, "PROTOBUF", list())
schema_registry_client.register_schema(subject_name="app-events-value", schema=p_schema)

# COMMAND ----------

df_fake_data = df_fake_data.withColumn("proto_payload", PF.to_protobuf(F.col("event"), options = schema_registry_conf))

# COMMAND ----------

# DBTITLE 1,To keep it simple, we're saving to Delta. IRL, you would not do this (you'd publish to Kafka, etc.)
(
  df_fake_data
    .select("proto_payload")
    .writeTo(f"proto_demo_{my_name}")
    .createOrReplace()
)

# COMMAND ----------

# DBTITLE 1,Let's look at the raw contents that were persisted
display(spark.read.table(f"proto_demo_{my_name}").limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC # Deserialization demo
# MAGIC Now we're going to read and deserialize the protobuf data

# COMMAND ----------

df_read_stream = spark.readStream.table(f"proto_demo_{my_name}")

# COMMAND ----------

# DBTITLE 1,Deserialize with from_protobuf
df_read_stream = df_read_stream.withColumn("deserialized_event", PF.from_protobuf(F.col("proto_payload"), options = schema_registry_conf))

# COMMAND ----------

# DBTITLE 1,Review the streaming data
display(df_read_stream.select("deserialized_event"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema evolution demo
# MAGIC Now let's add a new field to the schema. We'll add the new field: `email` and, since we're adding a field, we can rest assured that the schema change is compatible.
# MAGIC
# MAGIC ## Note: Compatibility settings
# MAGIC Confluent's Schema Registry allows you to choose from a number of compatibility settings. "Backward" is the default. The definition: "Consumers using the new schema can read data written by producers using the latest registered schema."

# COMMAND ----------

protodef = """
syntax = "proto3";

package demo;

message event {
  optional int64 id = 1;
  optional string name = 2;
  optional string address = 3;
  optional string uuid = 4;
  optional string email = 5;
}
"""

# COMMAND ----------

# DBTITLE 1,Register the new schema version
p_schema = Schema(protodef, "PROTOBUF", list())
schema_registry_client.register_schema(subject_name="app-events-value", schema=p_schema)

# COMMAND ----------

# DBTITLE 1,Produce some new fake data (that includes the new email field)
df_fake_data = spark.range(10000)
df_fake_data = df_fake_data.withColumn("name", F.udf(fake_generator.name)())
df_fake_data = df_fake_data.withColumn("address", F.udf(fake_generator.address)())
df_fake_data = df_fake_data.withColumn("uuid", F.udf(fake_generator.uuid4)())
df_fake_data = df_fake_data.withColumn("email", F.udf(fake_generator.email)())
df_fake_data = df_fake_data.selectExpr("struct(*) as event")

# COMMAND ----------

df_fake_data = df_fake_data.withColumn("proto_payload", PF.to_protobuf(F.col("event"), options = schema_registry_conf))

# COMMAND ----------

# DBTITLE 1,Save the new data and scroll back up to review the streaming data
(
  df_fake_data
    .select("proto_payload")
    .writeTo(f"proto_demo_{my_name}")
    .option("mergeSchema", True)
    .append()
)
