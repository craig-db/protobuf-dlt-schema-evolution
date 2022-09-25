# Evolving protobuf and consuming in Databricks DLT
Stream protobuf messages into Databricks DLT and handle evolution of the schema with the help of the Confluent schema registry.

# Prerequisites
1. Kafka access (and credentials), saved as the following secrets. Sc
2. Confluent Schema Registry access (and credentials)



# Simulator overview
* The "Producer" notebook creates simulated messages and sends them to a Kafka topic. Use the widget values to increase the number of different schema versions used in the flow.
* The "DLT" notebook will be installed by the setup script as a DLT Pipeline.
* The "Setup" notebook will install the DLT notebook and set up a job that will restart it upon failure.

# Instructions
1. Run the "Setup" notebook after adjusting the widget values. If the widgets are not yet rendered, run the Setup notebook two times. One time to display the widgets and another time to trigger the installation process.
2. Run the "Producer" notebook. Check via DB SQL to see the resulting output in the "unified_gold" table.
3. Adjust the widget values (e.g. increase the number of versions). You should be able to see the job restart the pipeline and your DB SQL queries should reflect the evolved schema.

### Clean up
Stop and delete the job, pipeline and drop the target schema.