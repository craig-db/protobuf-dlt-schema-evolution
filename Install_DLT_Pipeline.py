# Databricks notebook source
# DBTITLE 1,The Databricks CLI lib will help with API calls to install the DLT pipeline
# MAGIC %pip install databricks_cli

# COMMAND ----------

# DBTITLE 1,Common variables and secrets...
# MAGIC %run "./Common"

# COMMAND ----------

# DBTITLE 1,Delta Live Pipeline name will be "Protobuf Example_<your user>"
PIPELINE_NAME = f"Protobuf_Games_Demo_{my_name}"

# COMMAND ----------

# DBTITLE 1,Databricks API-related libraries
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.pipelines.api import PipelinesApi

# COMMAND ----------

# DBTITLE 1,Intitialize Client
nb_context = dbutils.entry_point.getDbutils().notebook().getContext()
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

# DBTITLE 1,Note the DLT pipeline ID
print(retval)
