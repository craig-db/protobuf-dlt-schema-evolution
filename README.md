# Instructions
1. Review all of the notebook code. Use the Databricks Assitant to help you understand the code.
2. Set the secrets needed for the demo. See the [Common]($./Common) notebook for the required secret values and the secret scope.
3. Run the [Producer]($./Producer) notebook once. This will stream some data into Kafka.
4. Run the [Install_DLT_Pipeline]($./Install_DLT_Pipeline) notebook. This will install the Pipeline.
4. Go to the Delta Live Tables section of Databricks. Watch the progress of the pipeline.
5. Use the [Producer]($./Producer) notebook to generate more data. Adjust the `num_versions` widget to simulate schema evolution.
6. To stop the demo and clean up related artifacts: stop the DLT pipeline and run the [Cleanup]($./Cleanup) notebook.