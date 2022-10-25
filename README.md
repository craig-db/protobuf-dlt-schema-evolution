# Stream to Delta Lake with an optimized payload format: protocol buffers
The [Lakehouse platform](https://www.databricks.com/product/data-lakehouse) is a unified Data-to-AI platform for the enterprise where data engineers, data analysts, business analysts, and data scientists collaborate in a single platform. Streaming workloads in [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) pipelines can power near real-time prescriptive and [predictive analytics](https://www.databricks.com/glossary/predictive-analytics) and automatically retrain Machine Learning (ML) models using [Databricks built-in MLOps support](https://www.databricks.com/blog/2022/06/22/architecting-mlops-on-the-lakehouse.html). The models can be exposed as [scalable, serverless REST end-points](https://docs.databricks.com/mlflow/serverless-real-time-inference.html), all within the Databricks platform.

The data that makes up these streaming workloads may have many origins, from a variety of use cases. Examples of streaming use cases include:
|Streaming Data|Use Case|
|----|----|
|IoT sensors on manufacturing floor equipment|Generating predictive maintenance alerts and preemptive part ordering|
|Set-top box telemetry|Detecting network instability and dispatching service crews|
|Player metrics in a game|Calculating leader-board metrics and detecting cheat|

This data is often streamed through open source messaging layers to handle the transmission of the data payload from producers to consumers. [Apache Kafka](https://docs.databricks.com/structured-streaming/kafka.html) is a widely used messaging platform used for such payloads.

## Learning Objectives
In this blog post you will learn about:
* How [protocol buffers](https://developers.google.com/protocol-buffers) ("protobuf") are a compact serialization format
* How [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) ("DLT") pipelines provide a feature-rich platform for your ETL pipelines
* How to write a DLT pipeline to consume protobuf values from an Apache Kafka stream.
* How the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) is leveraged for decoding the binary message payload
* Addressing schema evolution in the DLT pipeline

### Disclaimer Regarding the Example Code
Example code is provided for demonstration purposes as part of this blog. The code is not an officially supported part of Databricks. It is for instructional purposes only. Although it functioned as part of this blog and tutorial, you may have to work through any issues on your own.

## Optimizing the Streaming Payload Format
Databricks Lakehouse provides capabilities that help optimize the AI journey by unifying Business Analysis, Data Science and Data Science activities in a single, governed platform. And when we consider optimizing the end-to-end technology stack, we naturally consider the serialization format of the message payload as one element of the stack subject to optimization.

This remainder of this blog post will focus on using an optimized format, created by Google, called [protocol buffers](https://developers.google.com/protocol-buffers). It will also explain how to harness the power of [Databricks Delta Live Tables](https://www.databricks.com/product/delta-live-tables) for its [autoscaling capabilities](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-concepts.html#databricks-enhanced-autoscaling) and ETL pipeline features, such as [orchestration, cluster management, monitoring, data quality, and error handling](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-concepts.html). Lastly, this blog post will explore the concept of [schema evolution](https://docs.databricks.com/ingestion/auto-loader/schema.html#how-does-auto-loader-schema-evolution-work) in the context of streaming workloads.

### What Makes Protobuf an "Optimized" Serialization Format?
Google enumerates the [advantages of protocol](https://developers.google.com/protocol-buffers/docs/overview) buffers as follows:
* Compact data storage
* Fast parsing
* Availability in many programming languages
* Optimized functionality through automatically-generated classes

A key aspect of the optimization usually involves use of pre-compiled classes that are used in the consumer and producer programs that a developer would write. In a nutshell, consumer and producer programs that leverage protobuf's are "aware" of a message structure, and the binary payload of a protobuf message benefits from:
* Positioing within the binary message, removing the need for field markers or delimeters
* Primitive data types

The client and producer programs leverage the compiled protobuf descriptions. The [protoc compiler](https://github.com/protocolbuffers/protobuf#protocol-compiler-installation) is used to compile those definitons into classes in a variety of languages, including Java and Python. For this example, the DLT code manage the compilation of the protocol definitions, and it will leverage Python as the target language for the compiler. To learn more about how protocol buffers work, go here: https://developers.google.com/protocol-buffers/docs/overview#work.

## Optimizing the Streaming Runtime
In addition Delta Live Tables (DLT) benefits for ETL development, DLT overcomes a limitation in Spark Structured Streaming archtectures that involves the downscaling of the compute resources (see [“Downscaling: The Achilles heel of Autoscaling Apache Spark Clusters”](https://www.youtube.com/watch?v=XA_oVcoK7b4) for more information). There are some known work-arounds that address the downscaling challenge (for example, [“A Self-Autoscaler for Structured Streaming Applications in Databricks”](https://medium.com/gumgum-tech/a-self-autoscaler-for-structured-streaming-applications-in-databricks-a8bc4601d1d1)). However, these work-arounds require additional complexity, code, and management.

So, DLT is a natural choice for long-running streaming workloads with variable volume. It will scale up and down the compute resources using [Enhanced Autoscaling](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-concepts.html#databricks-enhanced-autoscaling).

## Handling Payload Schema Evolution
Earlier we briefly described protobuf schema and how the protocol definition must be compiled into classes (e.g. Python or Java). The presents a challenge for those trying to design a continuously streaming application that handles the evolution of the schema.

### Exploring Payload Formats for IoT Streaming Data
Before proceed, it is worth mentioning that JSON or Avro may be suitable alternatives for streaming payload. These formats offer benefits that, for some use cases, may outweigh protobuf.
#### JSON
JSON is a great format for development because it is mostly human-readable. The other formats we'll explore are binary formats and require tools to inspect the underlying data values. Unlike Avro and protobuf, however, the JSON document is stored as a large string (potentially compressed) and this means more bytes may be used than what a value actually represents. Consider the short int value of 8. A short int typically requires two bytes. In JSON, you may have a document that looks like the following, and it will require a number of bytes for the associated key, quotes, etc.:
```
{
  "my_short": 8
}
```
Considering all the characters used above, we're looking at 17 bytes. When we consider protobuf, we would expect 2 bytes plus a few more, perhaps, for the overhead related to the positioning metadata.

##### JSON Support in Databricks
On the positive side, JSON documents have a rich set of benefits when used with Databricks. [Databricks Autoloader](https://docs.databricks.com/ingestion/auto-loader/index.html) can easily transform JSON to a structured DataFrame while also providing built-in support for:
* Schema inference - you can supply a schema when reading JSON into a DataFrame so that the target DataFrame or Delta table has the desired schema. Or you can let the engine [infer the schema](https://docs.databricks.com/ingestion/auto-loader/schema.html#syntax-for-schema-inference-and-evolution). [Schema hints](https://docs.databricks.com/ingestion/auto-loader/schema.html#override-schema-inference-with-schema-hints) can be supplied, alternatively, if want a balance of those features.
* [Schema evolution](https://docs.databricks.com/ingestion/auto-loader/schema.html#how-does-auto-loader-schema-evolution-work) - Autoloader provides options for how a workload should adapt to changes in the schema of incoming files.

Consuming and processing JSON in Databricks is simple. To create a Spark DataFrame from JSON files can be as simple as this:
```
df = spark.read.format("json").load("example.json")
```

#### Avro
Avro is an attractive serialization format because it is compact, it encompasses schema information in the files itselft, and it has [built-in support in Databricks that includes schema registry integration](https://docs.databricks.com/structured-streaming/avro-dataframe.html). [This tutorial](https://www.confluent.io/blog/consume-avro-data-from-kafka-topics-and-secured-schema-registry-with-databricks-confluent-cloud-on-azure/), co-authored by Databricks' Angela Chu, walks you through an example that leverages Confluent's Kafka and Schema Registry.

To explore an Avro-based dataset, it is as simple as working with JSON:
```
df = spark.read.format("avro").load("example.avro")
```

### Protobuf
[This datageeks.com article](https://dataforgeeks.com/data-serialisation-avro-vs-protocol-buffers/2015/) compares Avro and protobuf. It is worth a read, if you are on the fence between Avro and protbuf. Regarding performance, it describes protobuf as the "fastest amongst all". If speed outweighs other considerations, such as JSON and Avro's greater simplicity, protobuf may be the best choice.

# Overcoming Challenges when using Protobuf
Let's summarize some of the challenges that briefly touched upon earlier:
* Programs that leverage protobuf have to work with classes that were compiled using protoc. The protoc compiler is not installed, by default, on Databricks clusters
* No built-in support in Spark (as of 2022) for consuming protobuf.
* In a streaming application with protobuf messages that have evolving schema, protoc has to be leveraged as the application runs

Now, lets walk through these challenges and descibe how they can be overcome.

## Compiling the Protobuf Schema
The `protoc` call is costly, so we want to minimize the use. Ideally, it should be invoked once per schema version id. 

### Solution
To solve the challenge, the following is needed:
1. The protobuf compiler needs to be installed on the cluster. To solve this, an [init script](https://docs.databricks.com/clusters/init-scripts.html) can be used to manage the installation of the protoc compiler on the cluster's worker nodes. The script will be run as new nodes are provisioned and before the worker is made available to the Spark runtime.
</br>
*Code Example: init script to install the protobuf compiler*
```
#!/bin/sh
PB_REL="https://github.com/protocolbuffers/protobuf/releases"
curl -LO $PB_REL/download/v21.5/protoc-21.5-linux-x86_64.zip
unzip protoc-21.5-linux-x86_64.zip -d /usr/local/
```
Note: in DLT, you can configure the init script up by editing the JSON definition of the DLT pipeline.

2. As micro batches are processed, DLT may scale the cluster up or down. This means that the ability to receive the binary payload and decode it (using a compiled protobuf schema) needs to avoid the need to recompile the protobuf schema repeatedly. To address this, a function like the one below can help ensure the compiling action is minimized (by checking for the existence of the generated Python class). When used within an UDF, the distribution of the logic to new nodes will ensure the compilation happens once.
</br>
*Code Example: Compile once*
```
  def get_message_type(version_id, schema_str):
  mod_name = f'destination_{version_id}_pb2'

  tmpdir = "/tmp"
  fname = f"destination_{version_id}.proto"
  fpath = f"{tmpdir}/{fname}"
  mname = f'{fname.replace(".proto", "_pb2")}'

  if (not os.path.exists(f"{mname}.py")):
    f = open(fpath, "w")
    f.write(schema_str)
    f.close()
    retval = os.system(f"protoc -I={tmpdir} --python_out={tmpdir} {fpath}")
    print(f"retval: {retval} for path {fpath} for schema_str {schema_str}")
    if (retval != 0):
      raise Exception(f"protoc bombed with {retval}")
  if (tmpdir not in sys.path):
    sys.path.insert(0, tmpdir)
  
  pkg = importlib.import_module(mname)  

  return pkg
```

Notice the part with `protoc -I={tmpdir} --python_out={tmpdir} {fpath}`... This line of code makes a system call to invoke the protobuf compiler (protoc).

## Handling an Evolving Schema
The schema of the protobuf messages may evolve over time. Also, at any given time, the stream may consist of messages with a variety of schemas.

### Solution
To handle schema evolution, the following steps are taken:
Upon pipeline initialization, a call is made to the Confluent Schema Registry. This latest known schema version is captured.
</br>
*Code Example: get latest schema_id from the Schema Registry*
```
src = SchemaRegistryClient(schema_registry_conf)
raw_versions = src.get_versions(f"{TOPIC}-value")
latest_id = src.get_latest_version(f"{TOPIC}-value").schema_id
```

The latest Schema Id is looked up during the startup of the DLT pipeline. If a newer version is detected, the code for the DLT pipeline in this tutorial will cause the pipeline to fail. When in a DLT pipeline is set to run in [Production mode](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-concepts.html#development-and-production-modes) and it fails, the DLT pipeline will eventually restart and will use the newer Schema Id as it sets up the pipeline and target Delta table.


# Example Code to Demonstrate Schema-evolving ProtoBuf Messages and Consuming them in Databricks DLT
The purpose of this prototype is to demonstrate how you can stream protobuf messages into Databricks DLT and handle evolution of the schema with the help of the Confluent schema registry.

## The Code Files
Accompanying this repo, you will find three code files:
1. Install_DLT_Pipeline - this notebook should be run to install the DLT pipeline.
2. Producer - this notebook will generate fake data and is meant to be run multiple times. It provides a way to send `x` number of messages for `y` different schemas. 
3. DLT - this notebook is the code for the DLT pipeline.

### Read the comments
The code files include commentary and additional links to help you understand the various functions and steps in the process. Read through the comments and pay attention to the notebook cell titles.

# Prerequisites
The following Kafka and Schema Registry connection details (and credentials), should be saved as Databricks Secrets:
- SR_URL: Schema Registry URL (e.g. https://myschemaregistry.aws.confluent.cloud)
- SR_API_KEY: Schema Registry API Key 
- SR_API_SECRET: Schema Registry API Secret 
- KAFKA_KEY: Kafka API Key 
- KAFKA_SECRET: Kafka Secret 
- KAFKA_SERVER: Kafka host:port (e.g. mykafka.aws.confluent.cloud:9092)
- KAFKA_TOPIC: The Kafka Topic
- TARGET_SCHEMA: The target database where the streaming data will be appended into a Delta table (the destination table is named unified_gold)

Go here to learn how to save secrets: https://docs.databricks.com/security/secrets/index.html 

## Kafka and Schema Registry
The code in the example was written and tested using Confluent's hosted Kafka. To obtain a free trial Confluent account, go here: https://www.confluent.io/confluent-cloud/tryfree/. The code may or may not work seamlessly with other Kafka and Schema Registry providers.

# Notebook overview
* The "Producer" notebook creates simulated messages and sends them to a Kafka topic. Use the widget values to increase the number of different schema versions used in the flow and the number of records sent per notebook run.
* The "DLT" notebook will be installed as a Continuous DLT Pipeline.
* The "Install_DLT_Pipeline" notebook will install the aforementioned DLT notebook.

# Instructions
1. Run the "Producer" notebook. This will publish some messages to Kafka. The notebook also installs a script in DBFS that will be used by the DLT notebook. That "init script" will ensure that the protobuf compiler (protoc) is installed on the cluster that runs the DLT pipeline.
2. Run the "Install_DLT_Pipeline" notebook.
3. Go back to the "Producer" notebook. Adjust the widget values (e.g. increase the number of versions). If the DLT pipeline is configured to run in "Production" mode, you should be able to see the job automatically restart the pipeline.

# How it works
* When the DLT pipeline starts, it keeps track of the current maximum version schema identifier that it received from a call to the Confluent Schema Registry.
* If a newer Schema Identifier is detected, the pipeline fails. It should restart itself (assuming you use "Production" mode for the DLT pipeline).
* The reason the stream restarts is so that the target sink schema can get updated to the latest schema from the Schema Registry.

### Clean up
Stop and delete the job, pipeline and drop the target schema.