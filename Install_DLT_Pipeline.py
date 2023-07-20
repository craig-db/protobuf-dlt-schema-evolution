# Databricks notebook source
# MAGIC %pip install databricks_cli

# COMMAND ----------

# MAGIC %pip install confluent_kafka

# COMMAND ----------

# MAGIC %run "./Common"

# COMMAND ----------

# DBTITLE 1,Delta Live Pipeline name will be "Protobuf Example_<your user>"
PIPELINE_NAME = f"Protobuf_Games_Demo_{my_name}"

# COMMAND ----------

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.pipelines.api import PipelinesApi

# COMMAND ----------

nb_context = dbutils.entry_point.getDbutils().notebook().getContext()

# COMMAND ----------

#Intitialize Client
api_client = ApiClient(token = nb_context.apiToken().get(), host = nb_context.apiUrl().get())
pipelines_api = PipelinesApi(api_client)

# COMMAND ----------

# DBTITLE 1,Build the path to the DLT notebook by using the path of this notebook.
dlt_nb_path = nb_context.notebookPath().getOrElse(None).replace("Install_DLT_Pipeline", "DLT")

# COMMAND ----------

# DBTITLE 1,Use Databricks API to register and start the DLT Pipeline
retval = pipelines_api.create(
  settings = {
    "name": PIPELINE_NAME,
    "catalog": CATALOG,
    "target": TARGET_SCHEMA,
    "configuration": {
        "games": f"{','.join(GAMES_ARRAY)}",
        "kafka_topic": WRAPPER_TOPIC
    },
    "development": True, 
    "continuous": True, 
    "channel": "PREVIEW",
    "libraries": [
      {
        "notebook": {
          "path": dlt_nb_path
        }
      }
    ]
    },
    settings_dir=None, 
    allow_duplicate_names=False
)

# COMMAND ----------

print(retval)

# COMMAND ----------


