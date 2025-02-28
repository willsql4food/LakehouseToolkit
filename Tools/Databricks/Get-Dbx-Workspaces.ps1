###############################################################################
# Get-Dbx-Workspaces
#
# Polls Databricks for all workspaces and the resulting definitions in a 
# JSON file
###############################################################################

# Workspaces are at the account level, not accessed through a given workspace
$dbxEnv = "ACCOUNT"

# Get the workspaces in this Databricks account
$linesep = "======================================================================"
Write-Output $linesep
Write-Output "Getting WORKSPACE list (using Databricks profile ACCOUNT)"
Write-Output $linesep

# Setup the script to invoke
$invoke = @{ 
    ScriptBlock = { databricks account workspaces list --profile $($args[0]) --output json }
    ArgumentList = $dbxEnv
}

# Get Workspaces, write to file & message to user
$file = "./Workspaces.$dbxEnv.json"
Invoke-Command @invoke | Set-Content -Path $file

Write-Output $linesep
Write-Output "Workspace definitions written to [$((Get-ChildItem $file).FullName)]"
Write-Output $linesep
