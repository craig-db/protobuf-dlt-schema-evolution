# Databricks notebook source
# MAGIC %pip install --upgrade protobuf

# COMMAND ----------

# MAGIC %pip install --upgrade pbspark

# COMMAND ----------

# MAGIC %pip install --upgrade  confluent_kafka

# COMMAND ----------

# DBTITLE 1,Variables for the producer
NUM_VERSIONS=6
NUM_RECORDS_PER_VERSION=10

# COMMAND ----------

protodef = """
syntax = "proto2";

package example{version};

message Person {{
  optional int64 id = 1;
  optional string name = 2;
  optional string email = 3;
  optional int64 quantity = 4;
{extras}
}}
"""

# COMMAND ----------

init_script_contents = """
#!/bin/sh

PC=`which protoc`
if [ $? -eq 1 ] 
then
  cd /
  PB_REL="https://github.com/protocolbuffers/protobuf/releases"
  curl -LO $PB_REL/download/v21.5/protoc-21.5-linux-x86_64.zip
  unzip -o /protoc-21.5-linux-x86_64.zip -d /usr/local/
fi

"""

dbutils.fs.put("dbfs:/FileStore/install_proto.sh", init_script_contents, True)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC . /dbfs/FileStore/install_proto.sh

# COMMAND ----------

SR_URL = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_URL")
SR_API_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_KEY")
SR_API_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "SR_API_SECRET")
KAFKA_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")
KAFKA_TOPIC = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_TOPIC")

# COMMAND ----------

# Required connection configs for Kafka producer, consumer, and admin

config = {
  "bootstrap.servers": f"{KAFKA_SERVER}",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": f"{KAFKA_KEY}",
  "sasl.password": f"{KAFKA_SECRET}",
  "session.timeout.ms": "45000"
}  

schema_registry_conf = {
    'url': SR_URL,
    'basic.auth.user.info': '{}:{}'.format(SR_API_KEY, SR_API_SECRET)}

# COMMAND ----------

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

import os, sys
import importlib

# COMMAND ----------

def get_schema_str(version):
  extras = list()
  for i in range(0, version + 1):
    extras.append(f"  optional float measure{i} = {i + 5};")
    
  return protodef.format(version=version, extras=("\n".join(extras)))

# COMMAND ----------

def generate_proto(schema_str, version_id):
  mod_name = f'destination_{version_id}_pb2'
  if (mod_name in sys.modules):
    del sys.modules[mod_name]
    del mod_name

  schema = Schema(schema_str, "PROTOBUF", list())
  tdir = "/tmp"
  fname = f"destination_{version_id}.proto"
  fpath = f"{tdir}/{fname}"
  f = open(fpath, "w")
  f.write(schema_str)
  f.close()
  cmd = f"protoc -I={tdir} --python_out={tdir} {fpath}"
  retval = os.system(cmd)
  sys.path.insert(0, '/tmp')
  print(f"retval={retval} for cmd={cmd} for schema_str={schema_str}")
  if retval != 0:
    raise Exception("Protobuf compilation failed. Fix this before proceeding.")
  
  pkg = importlib.import_module(mod_name)
  person = pkg.Person(
    id = (1234 + version_id), 
    name = f"John{version_id} Doe", 
    email = f"jdoe{version_id}@example.com", 
    quantity = (5678 + version_id)
  )
  setattr(person, f"measure{version_id}", version)
  
  return (pkg, person)

# COMMAND ----------

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# COMMAND ----------

a = AdminClient(config)

fs = a.create_topics([NewTopic(
     KAFKA_TOPIC,
     num_partitions=1,
     replication_factor=3
)])

# COMMAND ----------

for version in range(0, NUM_VERSIONS):
  print(f"prep version {version}")
  schema_str = get_schema_str(version)
  # print(schema_str)
  (pkg, protoclass) = generate_proto(schema_str, version)
  person = getattr(pkg, 'Person')
  ser = ProtobufSerializer(person, schema_registry_client, {'use.deprecated.format': False})
  config['value.serializer'] = ser
  producer = SerializingProducer(config)
  for rec in range(0, NUM_RECORDS_PER_VERSION):
    print(f"publishing {str(protoclass)}")
    ret = producer.produce(KAFKA_TOPIC, key=f"{version}.{rec}", value=protoclass)
  producer.flush()
