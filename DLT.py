# Databricks notebook source
import dlt

# COMMAND ----------

WRAPPER_TOPIC = spark.conf.get("kafka_topic")

# COMMAND ----------

GAMES_ARRAY = spark.conf.get("games").split(",")

# COMMAND ----------

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

# DBTITLE 1,The source view, consuming the Kafka messages and decoding the Schema Id of the payload
@dlt.view
def bronze_events():
  return (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(KAFKA_KEY, KAFKA_SECRET))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("subscribe", WRAPPER_TOPIC)
    .load()
    # The `binary_to_string` UDF helps to extract the Schema Id of each payload:
    .withColumn('decoded', from_protobuf(F.col("value"), options = schema_registry_options))
    .selectExpr("decoded.*")
  )

# COMMAND ----------

# DBTITLE 1,Gold table with the transformed protobuf messages, surfaced in a Delta table
for game in GAMES_ARRAY:
  @dlt.table(
    name = f"silver_{game}_events"
  )
  def gold_unified():
    return dlt.read_stream("bronze_events").where(F.col("game_name") == game)

# COMMAND ----------

for game in GAMES_ARRAY:
  @dlt.table(
    name = f"gold_{game}_player_agg"
  )
  def gold_unified():
    return spark.sql(f"""
      select gamer_id, count(event_timestamp) event_count, 
             max(event_timestamp) max_event_timestamp, 
             min(event_timestamp) min_event_timestamp
        from STREAMING(LIVE.silver_{game}_eventsWRAPPER_TOPIC = "protobuf_game_stream")
        group by gamer_id
    """)
