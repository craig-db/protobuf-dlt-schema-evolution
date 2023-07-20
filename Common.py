# Databricks notebook source
CATALOG = "craig_lukasik" # Protobuf_Games_Demo_{my_name}

# COMMAND ----------

WRAPPER_TOPIC = "protobuf_game_stream"

# COMMAND ----------

GAMES_ARRAY = [
  "rummy", "spades", "euchre", "go_fish", "uno", "hearts", "poker"
]

# COMMAND ----------

SR_URL = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_URL")
SR_API_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_KEY")
SR_API_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_SECRET")
KAFKA_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")

config = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": f"{KAFKA_KEY}",
  "sasl.password": f"{KAFKA_SECRET}",
  "session.timeout.ms": "45000"
}  

# COMMAND ----------

schema_registry_options = {
  "schema.registry.subject" : f"{WRAPPER_TOPIC}",
  "schema.registry.address" : f"{SR_URL}",
  "confluent.schema.registry.basic.auth.credentials.source" : "USER_INFO",
  "confluent.schema.registry.basic.auth.user.info" : f"{SR_API_KEY}:{SR_API_SECRET}"
}

schema_registry_conf = {
  "url": SR_URL,
  "basic.auth.user.info": '{}:{}'.format(SR_API_KEY, SR_API_SECRET)
}

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.admin import AdminClient, NewTopic

# COMMAND ----------

admin_client = AdminClient(config)

# COMMAND ----------

def register_schema(topic, schema):
  schema_registry_client = SchemaRegistryClient(schema_registry_conf)
  k_schema = Schema(schema, "PROTOBUF", list())
  schema_id = int(schema_registry_client.register_schema(f"{topic}-value", k_schema))
  schema_registry_client.set_compatibility(subject_name=f"{topic}-value", level="FULL")

# COMMAND ----------

my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

# COMMAND ----------

TARGET_SCHEMA = f"protobuf_demo_{my_name}"
