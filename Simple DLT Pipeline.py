# Databricks notebook source
import dlt
import pyspark.sql.functions as F
import pyspark.sql.protobuf.functions as PF

# COMMAND ----------

KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")
KAFKA_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
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

@dlt.table
def bronze_deserialized():
  df = (
    spark.readStream.format("kafka")
      .option("startingOffsets", "earliest")
      .option("subscribe", "app-events")
      .option("kafka.bootstrap.servers", KAFKA_SERVER)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config", 
              f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{KAFKA_KEY}' password='{KAFKA_SECRET}';")
      .option("kafka.ssl.endpoint.identification.algorithm", "https")
      .option("kafka.sasl.mechanism", "PLAIN")
      .load()
  ) 
  return (
    df
      .withColumn("deserialized_event", PF.from_protobuf(F.col("value"), options = schema_registry_conf))
      .selectExpr("deserialized_event.*")
  )

# COMMAND ----------


