###############################################################################
# Get-Dbx-Catalogs
#
# Polls Databricks for all CATALOGS with their workspace bindings and stores
# the definitions in a JSON file
###############################################################################

# Command line arguments - first item is expected to be environment
$dbxEnv = "DEFAULT"
if ($null -ne $args[0])
{
    $dbxEnv = $args[0]
}

# Get the catalogs in this Databricks account - explicitly request JSON out
$linesep = "======================================================================"
Write-Output $linesep
Write-Output "Getting CATALOG list (using Databricks profile $dbxEnv)"
Write-Output $linesep

$invoke = @{ 
    ScriptBlock = { databricks catalogs list --profile $($args[0]) --output json }
    ArgumentList = $dbxEnv
}
$catlist = Invoke-Command @invoke | ConvertFrom-Json

# Setup final array to hold catalog information list
$cats = @()

# Loop over the catalogs and extract the name, type, and comment
# Split on <space> gives an array of elements, 
# but comment is sliced up and will have to be reassembled

########################
# Testing
$i = 1
while ($i -le 4 )
########################

# $i = 0
#while ($i -le $catlist.count)
{
    # The result has a header line, so ignore first 'catalog' returned
    $i++

    # # New object to hold this catalog
    # $cat = [PSCustomObject]@{
    #     name = ""
    #     full_name = ""
    #     share_name = ""
    #     catalog_type = ""
    #     isolation_mode = ""
    #     metastore_id = ""
    #     provider_name = ""
    #     securable_kind = ""
    #     securable_type = ""
    #     comment = ""
    #     created_at = 0
    #     created_by = ""
    #     updated_at = 0
    #     updated_by = ""
    #     bindings = @()
    #     privilege_assignments = @()
    #     schemas = @()
    # }

    $cat = $catlist[$i] 
    $cat | Add-Member -MemberType NoteProperty -Name "bindings" -Value ([PSCustomObject]@())
    $cat | Add-Member -MemberType NoteProperty -Name "privilege_assignments" -Value ([PSCustomObject]@())
    $cat | Add-Member -MemberType NoteProperty -Name "schemas" -Value ([PSCustomObject]@())

    Write-Output "$($i): $($cat.name) $linesep"
    Write-Output $cat | ConvertTo-Json -depth 32
}
