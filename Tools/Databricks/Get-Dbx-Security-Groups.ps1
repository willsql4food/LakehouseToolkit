###############################################################################
# Get-Dbx-Security-Groups
#
# Polls Databricks for all GROUPS, then iterates over these to find and store
# the definitions in a set of JSON files
###############################################################################

# Security groups are at the account level, not accessed through a given workspace
$dbxEnv = "ACCOUNT"

# Get the groups in this Databricks account
$linesep = "======================================================================"
Write-Output $linesep
Write-Output "Getting GROUP list (using Databricks profile $dbxEnv)"
Write-Output $linesep

$invoke = @{ 
    ScriptBlock = { databricks account groups list --profile $($args[0]) --output json}
    ArgumentList = $dbxEnv
}

$grplist = Invoke-Command @invoke | ConvertFrom-Json

# Setup final array to hold catalog information list
$groups = @()

# Loop over the groups and extract the ID & name
foreach ($g in $grplist)
{
    # Now get the full definition, membership, etc. for the group
    if ($g.dispalyName -ne "")
    {
        #----------------------------------------------------------------------
        # Group details
        #----------------------------------------------------------------------
        # Add object attributes to capture externalId, members and schemas
        $g | Add-Member -MemberType NoteProperty -Name "externalId" -Value ""
        $g | Add-Member -MemberType NoteProperty -Name "members" -Value ([PSCustomObject]@())
        $g | Add-Member -MemberType NoteProperty -Name "schemas" -Value ([PSCustomObject]@())

        # Get the properties of the group
        Write-Output "- Getting group definition for $($g.displayName)"
        $invoke = @{ 
            ScriptBlock = { databricks account groups get $($args[1]) --profile $($args[0]) }
            ArgumentList = $dbxEnv, $g.id
        }
        $grpdetail = Invoke-Command @invoke | ConvertFrom-Json
    
        # Augment the current group with these atrributes
        $g.externalId = $grpdetail.externalId
        $g.members = $grpdetail.members
        $g.schemas = $grpdetail.schemas
        
        # Add augmented group to final array
        $groups += $g
    }
}

# Store group list and return message to user
$file = "./Security-Groups.$dbxENV.json"
$groups | ConvertTo-Json -depth 32 | Set-Content -Path $file

Write-Output $linesep
Write-Output "$($groups.count) Security Groups written to [$((Get-ChildItem $file).FullName)]"
Write-Output $linesep
