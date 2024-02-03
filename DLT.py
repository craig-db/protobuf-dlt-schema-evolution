# Databricks notebook source
# DBTITLE 1,DLT is Databricks flagship ETL framework
import dlt

# COMMAND ----------

# DBTITLE 1,Kafka topic
WRAPPER_TOPIC = spark.conf.get("kafka_topic")

# COMMAND ----------

# DBTITLE 1,The game data will consist of the games passed in a variable to the pipeline
GAMES_ARRAY = spark.conf.get("games").split(",")

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
    spark
    .readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
    .withColumn('decoded', from_protobuf(F.col("value"), options = schema_registry_options))
    .selectExpr("decoded.*")
  )

# COMMAND ----------

# DBTITLE 1,Silver Delta tables for each game. Notice the "for" loop and how it simplifies creating a number of tables at once
for game in GAMES_ARRAY:
  @dlt.table(
    name = f"silver_{game}_events"
  )
  def gold_unified():
    return dlt.read_stream("bronze_events").where(F.col("game_name") == game)

# COMMAND ----------

# DBTITLE 1,Likewise, a loop creates the gold, materialized views for all the games
for game in GAMES_ARRAY:
  @dlt.table(
    name = f"gold_{game}_player_agg"
  )
  def gold_unified():
    return spark.sql(f"""
      select gamer_id, count(event_timestamp) event_count, 
             max(event_timestamp) max_event_timestamp, 
             min(event_timestamp) min_event_timestamp
        from LIVE.silver_{game}_events
        group by gamer_id
    """)
