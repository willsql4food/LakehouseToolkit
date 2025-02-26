$invoke = @{ 
    ScriptBlock = { databricks catalogs list }
}

if ($null -ne $args[0])
{
    $invoke = @{ 
        ScriptBlock = { databricks catalogs list --profile $($args[0]) }
        ArgumentList = $($args[0])
    }
}

###############################################################################
# Get-Dbx-Catalogs
#
# Polls Databricks for all CATALOGS with their workspace bindings and stores
# the definitions in a JSON file
###############################################################################

# Get the catalogs in this Databricks account
$linesep = "======================================================================"
Write-Output $linesep
Write-Output "Getting CATALOG list"
Write-Output $linesep

$catlist = Invoke-Command @invoke

$invoke.ScriptBlock = { databricks catalogs get silver_prod }

Write-Output $catlist