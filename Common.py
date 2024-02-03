# Databricks notebook source
# DBTITLE 1,Get your name
my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

# COMMAND ----------

# DBTITLE 1,Unique names for your catalog and schema
CATALOG = f"Protobuf_Games_Demo_{my_name}"
TARGET_SCHEMA = f"protobuf_demo_{my_name}"

# COMMAND ----------

# DBTITLE 1,You should have permission to create a catalog. If not, adjust CATALOG and TARGET_SCHEMA variables (set in previous cell)
spark.sql(f"create catalog if not exists {CATALOG}")

# COMMAND ----------

# DBTITLE 1,The Kafka topic used for this demo
WRAPPER_TOPIC = "protobuf_game_stream"

# COMMAND ----------

# DBTITLE 1,The games in the demo. Big hits (in the 1950s, perhaps)
GAMES_ARRAY = [
  "rummy", "spades", "euchre", "go_fish", "uno", "hearts", "poker"
]

# COMMAND ----------

# DBTITLE 1,Secrets... shh!
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

# DBTITLE 1,Setup Kafka and Schema Registry connection parameters
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
