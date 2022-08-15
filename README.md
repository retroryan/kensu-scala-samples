# kensu-scala-samples

Samples of using the kensu agent with Scala and Spark


## Overview of Kensu agent

The agent collects information inside the application to send it to Kensu. The agent augments the program library and gathers metadata, defines the lineage and computes the metrics before sending this to Kensu through the API. To configure the agent, a configuration file is needed.

# Financial Data Report Sample

## Setup

1. Setup an account on the [Kensu Sandbox](https://sandbox.kensuapp.com/)
1. Get an api-token from Preferences -> Ingestion token
1. Copy the conf-template.ini to conf.ini and paste the Ingestion token for the value of dam.ingestion.auth.token

## Run it with the spark shell


```spark-shell -i --jars kensu-dam-spark-collector-0.24.1-alpha220811131741_spark-3.2.1.jar,sdk_2.12.jar load_financial_data_kensu.scala

spark-shell -i --jars kensu-dam-spark-collector-0.24.1-alpha220811131741_spark-3.2.1.jar,sdk_2.12.jar reporting_spark_v1.scala
```
