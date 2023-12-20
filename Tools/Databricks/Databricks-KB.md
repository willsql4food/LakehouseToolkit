# Databricks KB

## Secrets & Scopes - Setup Example
Here's what I had to do to get a new Azure Databricks workspace to be able to read from a storage account:
* Create Az Databricks workspace
* Generate a Personal Access Token and store in Azure Key Vault
* Generate a Shared Access Signature for the storage account and store in Azure Key Vault
* Install Databricks CLI (cloned the Git Repo https://github.com/databricks/setup-cli.git)
    * Also monkeyed around with curl for installation and querying secrets & scopes, YMMV
* Setup a ___.databrickscfg___ file to hold configuration information for my workspace
    * used `databricks config` command and filled in the two prompts (DBX instance and Personal Access Token above)
* Create a secret scope
    * `databricks secrets create-scope log_analytics --initial-manage-principal users`
* Create a secret in that scope
    * `databricks secrets put-secret log_analytics sas_key_staab09289802`
    * then supply the SAS key for the storage account