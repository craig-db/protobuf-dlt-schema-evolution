# Databricks notebook source
# MAGIC %pip install --upgrade  confluent_kafka

# COMMAND ----------

import dlt

# COMMAND ----------

#
# We avoid hard-coding URLs, keys, secrets, etc. by using Databricks Secrets. 
# Read about it here: https://docs.databricks.com/security/secrets/index.html
#

# Kafka-related settings
KAFKA_KEY = dbutils.secrets.get(scope = "dogfood-testing", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")

# Schema Registry-related settings
SR_URL = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_URL")
SR_API_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_KEY")
SR_API_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_SECRET")

# COMMAND ----------

#
# Kafka configuration
#
# Confluent recommends SASL/GSSAPI or SASL/SCRAM for production deployments. Here, instead, we use
# SASL/PLAIN. Read more on Confluent security here: 
# https://docs.confluent.io/platform/current/security/security_tutorial.html#security-tutorial
config = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": f"{KAFKA_KEY}",
  "sasl.password": f"{KAFKA_SECRET}",  
  "session.timeout.ms": "45000"
}  

#
# Schema Registry configuration
#
# For exploring more security options related to the Schema Registry, go here:
# https://docs.confluent.io/platform/current/schema-registry/security/index.html
schema_registry_conf = {
    'url': SR_URL,
    'basic.auth.user.info': '{}:{}'.format(SR_API_KEY, SR_API_SECRET)}

# COMMAND ----------

#
# We assume there will be a topic for each game and that the topic name will be of the format "game-<game name>".
# This list (GAME_STREAMS) will get populated by finding the topics that are in the schema registry where
# the topic name matches that format.
#
GAME_STREAMS = list()

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
subjects = schema_registry_client.get_subjects()

for subject in subjects:
  GAME_STREAMS.append(subject)

# COMMAND ----------

# DBTITLE 1,Necessary Imports
# Source for these libraries can be found here: https://github.com/confluentinc/confluent-kafka-python
from confluent_kafka import DeserializingConsumer, SerializingProducer

# https://github.com/confluentinc/confluent-kafka-python/tree/master/src/confluent_kafka/admin
from confluent_kafka.admin import AdminClient, NewTopic

# https://github.com/confluentinc/confluent-kafka-python/tree/master/src/confluent_kafka/schema_registry
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer, ProtobufDeserializer

# https://googleapis.dev/python/protobuf/latest/google/protobuf/message_factory.html
from google.protobuf.message_factory import MessageFactory

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType, BinaryType

# https://github.com/crflynn/pbspark
import pbspark

import os, sys, inspect, importlib

# COMMAND ----------

# DBTITLE 1,The source view, consuming the Kafka messages and decoding the Schema Id of the payload
@dlt.view
def kafka_source_table():
  return (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(KAFKA_KEY, KAFKA_SECRET))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("subscribe", KAFKA_TOPIC)
    .option("mergeSchema", "true")
    .load()
    # The `binary_to_string` UDF helps to extract the Schema Id of each payload:
    .withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
  )

# COMMAND ----------

# DBTITLE 1,Gold table with the transformed protobuf messages, surfaced in a Delta table
@dlt.table
def gold_unified():
  gold_df = spark.sql("""
  
    select a.valueSchemaId,
           b.schema_str,
           -- A new version will result in schema_str being NULL, thanks to the OUTER JOIN
           decode_proto_udf(a.valueSchemaId, b.schema_str, a.value) as decoded
      from STREAM(LIVE.kafka_source_table) a
      LEFT OUTER JOIN proto_schemas b on a.valueSchemaId = b.valueSchemaId
      
  """)
  
  return gold_df.select("decoded.*")
