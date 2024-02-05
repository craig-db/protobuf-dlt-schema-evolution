# Databricks notebook source
# DBTITLE 1,DLT is Databricks flagship ETL framework
import dlt

# COMMAND ----------

# DBTITLE 1,Kafka topic
WRAPPER_TOPIC = spark.conf.get("kafka_topic", "game-stream")

# COMMAND ----------

# DBTITLE 1,The game data will consist of the games passed in a variable to the pipeline
GAMES_ARRAY = spark.conf.get("games", "spades,hearts,blackjack").split(",")

# COMMAND ----------

# DBTITLE 1,Get secrets for the Confluent connections
#
# We avoid hard-coding URLs, keys, secrets, etc. by using Databricks Secrets. 
# Read about it here: https://docs.databricks.com/security/secrets/index.html
#

# Kafka-related settings
KAFKA_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")

# Schema Registry-related settings
SR_URL = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_URL")
SR_API_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_KEY")
SR_API_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_SECRET")

# COMMAND ----------

# DBTITLE 1,Setup the parameters needed to connect to Kafka and the Schema Registry
#
# Kafka configuration
#
# Confluent recommends SASL/GSSAPI or SASL/SCRAM for production deployments. Here, instead, we use
# SASL/PLAIN. Read more on Confluent security here: 
# https://docs.confluent.io/platform/current/security/security_tutorial.html#security-tutorial
kafka_options = {
    "kafka.bootstrap.servers": KAFKA_SERVER,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{KAFKA_KEY}' password='{KAFKA_SECRET}';",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "kafka.sasl.mechanism": "PLAIN",
    "subscribe": WRAPPER_TOPIC,
}

schema_registry_options = {
  "schema.registry.subject" : f"{WRAPPER_TOPIC}-value",
  "schema.registry.address" : f"{SR_URL}",
  "confluent.schema.registry.basic.auth.credentials.source" : "USER_INFO",
  "confluent.schema.registry.basic.auth.user.info" : f"{SR_API_KEY}:{SR_API_SECRET}"
}

# COMMAND ----------

# DBTITLE 1,Necessary Imports
import pyspark.sql.functions as F
from pyspark.sql.protobuf.functions import from_protobuf

# COMMAND ----------

# DBTITLE 1,The source bronze view, consuming the Kafka messages and deserializing the protobuf messages with from_protobuf
@dlt.view
def bronze_events():
  return (
    spark.readStream.format("kafka")
    .options(**kafka_options)
    .load()
    .withColumn('decoded', from_protobuf(F.col("value"), options = schema_registry_options))
    .selectExpr("decoded.*")
  )

# COMMAND ----------

# DBTITLE 1,Helper functions to define DLT tables
def build_silver(gname):
    @dlt.table(name=f"silver_{gname}_events")
    def gold_unified():
        return dlt.read_stream("bronze_events").where(F.col("game_name") == gname)
      
def build_gold(gname):
    @dlt.table(name=f"gold_{gname}_player_agg")
    def gold_unified():
        return (
            dlt.read(f"silver_{gname}_events")
            .groupBy(["gamer_id"])
            .agg(
                F.count("*").alias("session_count"),
                F.min(F.col("event_timestamp")).alias("min_timestamp"),
                F.max(F.col("event_timestamp")).alias("max_timestamp")
            )
        )

# COMMAND ----------

# DBTITLE 1,Silver Delta tables for each game. Notice the "for" loop and how it simplifies creating a number of tables at once
for game in GAMES_ARRAY:
    build_silver(game)
    build_gold(game)
