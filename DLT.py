# Databricks notebook source
# MAGIC %pip install --upgrade protobuf

# COMMAND ----------

# MAGIC %pip install pbspark

# COMMAND ----------

# MAGIC %pip install confluent_kafka

# COMMAND ----------

# MAGIC %pip install cachetools

# COMMAND ----------

import dlt

# COMMAND ----------

SR_URL = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_URL")
SR_API_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_KEY")
SR_API_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_SECRET")
KAFKA_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")
KAFKA_TOPIC = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_TOPIC")
TARGET_SCHEMA = dbutils.secrets.get(scope = "protobuf-prototype", key = "TARGET_SCHEMA")

# COMMAND ----------

# Required connection configs for Kafka producer, consumer, and admin

config = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": f"{KAFKA_KEY}",
  "sasl.password": f"{KAFKA_SECRET}",
  "session.timeout.ms": "45000"
}  

schema_registry_conf = {
    'url': SR_URL,
    'basic.auth.user.info': '{}:{}'.format(SR_API_KEY, SR_API_SECRET)}

# COMMAND ----------

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer, ProtobufDeserializer

from google.protobuf.message_factory import MessageFactory

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType, BinaryType

import pbspark

import os, sys
import importlib
from cachetools import cached

# COMMAND ----------

# DBTITLE 1,Transform the binary payload into a String
binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

# COMMAND ----------

# DBTITLE 1,Return the Python class for the associated schema; compile proto defn. with protoc, if the Python class is not detected
def get_message_type(version_id, schema_str):
  mod_name = f'destination_{version_id}_pb2'

  tmpdir = "/tmp"
  fname = f"destination_{version_id}.proto"
  fpath = f"{tmpdir}/{fname}"
  mname = f'{fname.replace(".proto", "_pb2")}'

  if (not os.path.exists(f"{mname}.py")):
    f = open(fpath, "w")
    f.write(schema_str)
    f.close()
    retval = os.system(f"protoc -I={tmpdir} --python_out={tmpdir} {fpath}")
    print(f"retval: {retval} for path {fpath} for schema_str {schema_str}")
    if (retval != 0):
      raise Exception(f"protoc bombed with {retval}")
  if (tmpdir not in sys.path):
    sys.path.insert(0, tmpdir)
  
  pkg = importlib.import_module(mname)  

  return pkg

# COMMAND ----------

# DBTITLE 1,Helper to find the Python class in the module that was created by protoc
import sys, inspect
def get_proto_class(pkg):
    for name, obj in inspect.getmembers(pkg):
        if inspect.isclass(obj):
            return obj
    return None

# COMMAND ----------

# DBTITLE 1,Decode the incoming payload and return a dictionary
def decode_proto(version_id, schema_str, binary_value):
  import os, pbspark, shutil
  # print(f"CALL decode_proto with version_id: {version_id}, schema_str: {schema_str}, binary_value: {binary_value}")
  mc = pbspark.MessageConverter()
  pkg = get_message_type(version_id, schema_str)
  p = get_proto_class(pkg)
  
  clazz = MessageFactory().GetPrototype(p.DESCRIPTOR)
  deser = ProtobufDeserializer(clazz, {'use.deprecated.format': False})
  if isinstance(binary_value, bytearray):
    binary_value = bytes(binary_value)

  mc = pbspark.MessageConverter()
  # print(f"ready to call deser.__call__ with {binary_value}")
  retval = mc.message_to_dict(deser.__call__(binary_value, None))

  return retval

# COMMAND ----------

# DBTITLE 1,Find starting and ending version details from the Schema Registry
src = SchemaRegistryClient(schema_registry_conf)
raw_versions = src.get_versions(f"{KAFKA_TOPIC}-value")
latest_id = src.get_latest_version(f"{KAFKA_TOPIC}-value").schema_id
starting_id = latest_id - len(raw_versions) + 1
print(f"starting_id: {starting_id}, latest_id: {latest_id}")

# COMMAND ----------

# DBTITLE 1,A helper DataFrame to help convey the proto schema to workers consuming the stream
idx = 0
schemas = list()
for version_id in range(starting_id, latest_id + 1):
  schema_str = src.get_schema(version_id).schema_str
  row = {
    "valueSchemaId": version_id,
    "schema_str": schema_str
  }
  schemas.append(row)

pkg = get_message_type((schemas[-1])['valueSchemaId'], (schemas[-1])['schema_str'])
p = get_proto_class(pkg)
mc = pbspark.MessageConverter()
spark_schema = mc.get_spark_schema(p.DESCRIPTOR)
  
df_schemas = spark.createDataFrame(data=schemas, schema="valueSchemaId BIGINT, schema_str STRING")


# COMMAND ----------

# DBTITLE 1,Register the previous function as a Spark UDF
decode_proto_udf = fn.udf(lambda x,y,z : decode_proto(x, y, z), spark_schema)
spark.udf.register("decode_proto_udf", f=decode_proto_udf)

# COMMAND ----------

# DBTITLE 1,The source view, using an expectation to fail the stream once a new version is detected
@dlt.view
@dlt.expect_or_fail("expected protobuf version", f"valueSchemaId <= {latest_id}")
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
    .withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
  )

# COMMAND ----------

df_schemas.createOrReplaceTempView("proto_schemas")

# COMMAND ----------

# DBTITLE 1,Gold table with the transformed protobuf messages surfaced in a Delta table
@dlt.table
def gold_unified():
  return spark.sql("""
    select a.valueSchemaId, b.schema_str, decode_proto_udf(a.valueSchemaId, b.schema_str, a.value) as decoded
      from STREAM(LIVE.kafka_source_table) a
      LEFT OUTER JOIN proto_schemas b on a.valueSchemaId = b.valueSchemaId
  """).select("decoded.*")
