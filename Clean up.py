# Databricks notebook source
# MAGIC %md
# MAGIC ### Clean up notebook
# MAGIC Use this notebook to clean up data in Kafka and drop the schema used for the demo.

# COMMAND ----------

# DBTITLE 1,Confluent library to delete the Kafka topic
# MAGIC %pip install --upgrade  confluent_kafka

# COMMAND ----------

# DBTITLE 1,Common variables / secrets
# MAGIC %run "./Common"

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

# COMMAND ----------

# DBTITLE 1,Delete the schemas from the Schema Registry
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
try:
  schema_registry_client.delete_subject(f"{WRAPPER_TOPIC}-value", True)
except Exception as e:
  print(f"ERROR: {e}")

# COMMAND ----------

# DBTITLE 1,Delete the topic from Kafka
from confluent_kafka.admin import AdminClient

admin_client = AdminClient(config)
try:
  admin_client.delete_topics([WRAPPER_TOPIC])
except Exception as e:
  print(f"ERROR: {e}")

# COMMAND ----------

# DBTITLE 1,Drop the schema used for the demo
spark.sql(f"drop database if exists {CATALOG}.{TARGET_SCHEMA} cascade")
