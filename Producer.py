# Databricks notebook source
# MAGIC %md
# MAGIC ### Simulator -- publish fake messages to Kafka
# MAGIC Use this notebook to publish messages to Kafka. Using the widgets:
# MAGIC 1. You can choose the number of messages to publish for each game 
# MAGIC 2. You can select the number of versions to generate
# MAGIC
# MAGIC The widgets will appear after you run the sixth cell of this notebook. The "Number of versions per game" widget helps to demonstrate schema evolution.
# MAGIC
# MAGIC #### Instructions
# MAGIC Run all the cells, one by one. You can generate more messages by re-running the last cell of this notebook.

# COMMAND ----------

# DBTITLE 1,Confluent_kafka library allows us to create topics and register schemas in Confluent
# MAGIC %pip install --upgrade confluent_kafka

# COMMAND ----------

# DBTITLE 1,Faker helps generate fake data for the simulator
# MAGIC %pip install faker

# COMMAND ----------

# DBTITLE 1,Set common variables and secret values
# MAGIC %run "./Common"

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.admin import AdminClient, NewTopic

# COMMAND ----------

# DBTITLE 1,Create the Kafka topic
admin_client = AdminClient(config)

fs = admin_client.create_topics([NewTopic(
    WRAPPER_TOPIC,
    num_partitions=1,
    replication_factor=3
)])

# COMMAND ----------

# DBTITLE 1,Create notebook widgets
dbutils.widgets.dropdown(
  name="num_records", 
  label="Number of Records per Game to Generate", 
  defaultValue="100", 
  choices=["100", "500", "1000", "5000", "10000"]
)

dbutils.widgets.dropdown(
  name="num_versions", 
  label="Number of Versions per Game to Generate", 
  defaultValue="2", 
  choices=["1", "2", "3", "4", "5"]
)

# COMMAND ----------

NUM_GAMES = len(GAMES_ARRAY)
NUM_RECORDS = int(dbutils.widgets.get("num_records"))
NUM_VERSIONS = int(dbutils.widgets.get("num_versions"))

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.protobuf.functions import to_protobuf
from faker import Faker

# COMMAND ----------

Faker.seed(999)
fake = Faker()

# COMMAND ----------

# Registry UDFs used by the simulator
fake_username = udf(fake.user_name)
fake_mac = udf(fake.mac_address)
fake_text = udf(fake.text)

# COMMAND ----------

# Used to track if a protobuf schema has already been registered in the Schema Registry
REGISTERED_SCHEMAS = {}

# COMMAND ----------

def register_schema(topic, schema):
  schema_registry_client = SchemaRegistryClient(schema_registry_conf)
  k_schema = Schema(schema, "PROTOBUF", list())
  schema_id = int(schema_registry_client.register_schema(f"{topic}-value", k_schema))
  schema_registry_client.set_compatibility(subject_name=f"{topic}-value", level="FULL")

# COMMAND ----------

"""
Generate fake records for a given game
"""
def generate_game_records(game_name, num_records, num_versions):
  proto_schema_arr = ["string game_name =1;", "string gamer_id =2;", 
                      "google.protobuf.Timestamp event_timestamp =3;", 
                      "string device_id =4;"]

  # To simulate schema evolution, newer versions get an additional column added
  for v in range(0, num_versions):
    proto_schema_arr.append(f"optional string col_custom_{v} ={int(v + 5)};") 

  # Construct the final protobuf schema definition
  proto_schema_str = str("\n".join(proto_schema_arr))
  proto_schema_str = f"""syntax = "proto3";
     import 'google/protobuf/timestamp.proto';
     
     message event {{
       {proto_schema_str}
     }}
  """
  if proto_schema_str not in REGISTERED_SCHEMAS:
    schema_id = register_schema(WRAPPER_TOPIC, proto_schema_str)
    REGISTERED_SCHEMAS[proto_schema_str] = schema_id  

  df = spark.range(num_records)
  df = df.withColumn("game_name", F.lit(game_name))
  df = df.withColumn("gamer_id", fake_username())
  df = df.withColumn("event_timestamp", F.current_timestamp())
  df = df.withColumn("device_id", fake_mac())
  for v in range(0, num_versions):
    df = df.withColumn(f"col_custom_{v}", fake_text())
  df = df.drop("id")
  
  return df

# COMMAND ----------

sr_conf = schema_registry_options.copy()
sr_conf["schema.registry.subject"] = f"{WRAPPER_TOPIC}-value"

# COMMAND ----------

# sc.setJobDescription("Write simulated records to Kafka")
for version in range(0, NUM_VERSIONS):
  df = None
  for game_num in range(0, NUM_GAMES):
    i_df = generate_game_records(GAMES_ARRAY[game_num], NUM_RECORDS, version)
    if df == None:
      df = i_df
    else:
      df = df.union(i_df)

  df = df.selectExpr("struct(*) as structs")
  df = df.withColumn("payload", to_protobuf("structs", options = sr_conf))
  df = df.selectExpr("'game_event' as key", "cast(payload as string) as value")

  (
    df
      .write
      .format("kafka")
      .option("topic", WRAPPER_TOPIC)
      .option("kafka.bootstrap.servers", KAFKA_SERVER)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config", 
              "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
              KAFKA_KEY, KAFKA_SECRET))
      .option("kafka.ssl.endpoint.identification.algorithm", "https")
      .option("kafka.sasl.mechanism", "PLAIN")
      .save()
  )

# COMMAND ----------


