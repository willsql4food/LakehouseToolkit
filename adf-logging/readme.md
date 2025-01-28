# ADF Logging, Metrics and Diagnostics

## To Azure Monitor & Log Analytics
1. [Configure diagnostics emmission](https://learn.microsoft.com/en-us/azure/data-factory/monitor-configure-diagnostics)
    1. Resource-Specific mode is ___strongly___ suggested
1. Wait 15 minutes...
1. Query the logs in either ADF or Log Analytics workspace

## Archive To Storage Account
1. [Configure diagnostics emmission](https://learn.microsoft.com/en-us/azure/data-factory/monitor-configure-diagnostics)
    1. Storage account must be in same region as data factory

## Analysis
Learn KQL (Kusto Query Language)

[Overview](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/)

[Query Tutorial](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/tutorial)

[Common Operators](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/tutorials/learn-common-operators)

[Coming from SQL](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/sqlcheatsheet)