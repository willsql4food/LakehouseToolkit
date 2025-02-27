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

$invoke = @{ 
    ScriptBlock = { databricks catalogs list --profile $($args[0]) --output json }
    ArgumentList = $dbxEnv
}
$catlist = Invoke-Command @invoke | ConvertFrom-Json

Write-Output "`t$($catlist.count) found"
Write-Output $linesep

# Setup final array to hold catalog information list
$cats = @()
$i = 0

# Loop over the catalogs and extract the basic attributes
#------------------------------------------------------------------------------
# For testing, put array brackets to restrict list of catalogs to operate on
# foreach ($cat in $catlist[2..3])
#------------------------------------------------------------------------------

foreach ($cat in $catlist)
{
    # Counter just for reporting
    $i++
    Write-Output "$i : $($cat.name)"

    # Make sure we have an actual catalog
    if ($cat.name -ne "")
    {
        #----------------------------------------------------------------------
        # Workspace bindings
        #----------------------------------------------------------------------
        # Add object attribute to capture Workspace bindings
        $cat | Add-Member -MemberType NoteProperty -Name "bindings" -Value ([PSCustomObject]@())

        Write-Output "`t- Getting workspace bindings"
        $invoke = @{ 
            ScriptBlock = { databricks workspace-bindings get-bindings catalog $($args[1]) --profile $($args[0]) }
            ArgumentList = $dbxEnv, $cat.name
        }
        $cat.bindings = Invoke-Command @invoke | ConvertFrom-Json
    
        #----------------------------------------------------------------------
        # Granted permissions
        #----------------------------------------------------------------------
        # Add object attribute to capture privilege assignments
        $cat | Add-Member -MemberType NoteProperty -Name "privilege_assignments" -Value ([PSCustomObject]@())

        Write-Output "`t- Getting permission grants"
        $invoke = @{ 
            ScriptBlock = { databricks grants get catalog $($args[1]) --profile $($args[0]) }
            ArgumentList = $dbxEnv, $cat.name
        }
        $cat.privilege_assignments = Invoke-Command @invoke | ConvertFrom-Json
    
        #----------------------------------------------------------------------
        # Schemas - explicitly ask for JSON out
        #----------------------------------------------------------------------
        # Add object attribute to capture Schemas
        $cat | Add-Member -MemberType NoteProperty -Name "schemas" -Value ([PSCustomObject]@())

        Write-Output "`t- Getting schemas"
        $invoke = @{ 
            ScriptBlock = { databricks schemas list $($args[1]) --profile $($args[0]) --output json }
            ArgumentList = $dbxEnv, $cat.name
        }
        $cat.schemas =  Invoke-Command @invoke | ConvertFrom-Json

        #----------------------------------------------------------------------
        # As long as the extracted catalog information is real, 
        # add it to the final list
        #----------------------------------------------------------------------
        if ($cat.name.Length -gt 0)
        {
            $cats += $cat
        }
    }
}

#------------------------------------------------------------------------------
# Store Catalogs and return message to user
$file = "./Catalogs.$($dbxEnv).json"
$cats | ConvertTo-Json -depth 32 | Set-Content -Path $file

Write-Output $linesep
Write-Output "$($cats.count) Catalogs written to [$((Get-ChildItem $file).FullName)]"
Write-Output $linesep
