# Azure Resource Logs - Archive and Analyze

## Setup
We need to configure our Azure resources to emit their logs to a blob storage account.  A given Diagnostic setting can emit logs to multiple targets; this topic is only concerned with sending log data to a Storage Account.

1. In the resource (Azure Data Factory, Storage Account, etc.) click `Monitoring > Diagnostic settings` 
1. Either add a diagnostic setting or edit an existing one
    1. Select the Categories of log events to emit
    1. Select the Metrics to emit
    1. In _Destination details_, check _Archive to a storage account_ and choose the appropriate subscription and storage account.
        * NOTE - the storage account must reside in the same location as the resource itself (eg. both reside in US East)

## Analysis Environment - Databricks
### Setup
1. Setup scope/secret so Databricks workspace can access the storage account (ref: [Databricks-KB](Databricks-KB.md))
1. In a Databricks notebook, set the Spark configuration to access the storage account using the authentication method and secrets available
1. Determine the filepath to the JSON documents emitted by the chosen resource(s)
    * _Liberal use of wild cards may be appropriate._
1. Build your analysis using Spark / data frames / SQL as desired.
    * NOTE - Many small JSON files make for inefficient processing - consider importing them to parquet files initially and building analysis off those.
        * Perhaps all JSON files emitted by a given resource in a day, week, or month are consolidated into a single parquet file and a retention policy is built around this.

