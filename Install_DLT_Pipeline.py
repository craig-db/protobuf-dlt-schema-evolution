# Databricks notebook source
# DBTITLE 1,Adjust this value to avoid conflicts
PIPELINE_NAME = "CraigLukasikProtoExample"

# COMMAND ----------

TARGET_SCHEMA = dbutils.secrets.get(scope = "protobuf-prototype", key = "TARGET_SCHEMA")

# COMMAND ----------

from databricks_cli.sdk.service import DeltaPipelinesService
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import get_config

pipeline_service = DeltaPipelinesService(_get_api_client(get_config()))

# COMMAND ----------

dlt_nb_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).replace("Install_DLT_Pipeline", "DLT")

# COMMAND ----------

retval = pipeline_service.create(
  name=PIPELINE_NAME, 
  target=TARGET_SCHEMA,
  development=True, 
  continuous=True, 
  libraries=[
    {
      "notebook": {
        "path": dlt_nb_path
      }
    }
  ],
  clusters=[
        {
            "label": "default",
            "init_scripts": [
                {
                    "dbfs": {
                        "destination": "dbfs:/FileStore/install_protoc.sh"
                    }
                }
            ],
            "autoscale": {
                "min_workers": 1,
                "max_workers": 2,
                "mode": "LEGACY"
            }
        }
    ]
)

# COMMAND ----------

print(retval)
