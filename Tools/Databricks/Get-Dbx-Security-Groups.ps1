###############################################################################
# Get-Dbx-Security-Groups
#
# Polls Databricks for all GROUPS, then iterates over these to find and store
# the definitions in a set of JSON files
###############################################################################

# Command line arguments - first item is expected to be environment
$dbxEnv = "ACCOUNT"

# Get the groups in this Databricks account
$linesep = "======================================================================"
Write-Output $linesep
Write-Output "Getting GROUP list (using Databricks profile $dbxEnv)"
Write-Output $linesep

$invoke = @{ 
    ScriptBlock = { databricks account groups list --profile $($args[0]) }
    ArgumentList = $dbxEnv
}

$grplist = Invoke-Command @invoke

# Results returned from Databricks CLI are in the form:
# ID   Name

# Setup final array to hold catalog information list
$groups = @()

# Loop over the groups and extract the ID & name
foreach ($ln in $grplist)
{
    # New object to hold this group
    $group = [PSCustomObject]@{
        ID = 0
        Name = ""
        MemberCount = 0
        externalId = ""
    }

    # Split the line by spaces and iterate over the parts
    $e = 0
    foreach ($element in $ln -split " ")
    {
        # If elements is blank, screen it out
        if ($element -ne "")
        {
            # First non-blank element is ID
            if($e -eq 0)
            {
                $group.ID = $element
                $group.Name = ''
            }
            # Subsequent elements make up Name
            if($e -ge 1) 
            {
                $group.Name += "$element "
            }

            # Only iterate if we found an element
            $e++
        }
        # Remove trailing space from Name
        $group.Name = $group.Name.Trim()
    }

    # Now get the full definition, membership, etc. for the group
    if ($group.Name -ne "")
    {
        Write-Output "- Getting group definition for $($group.Name)"

        $invoke = @{ 
            ScriptBlock = { databricks account groups get $($args[1]) --profile $($args[0]) }
            ArgumentList = $dbxEnv, $group.ID
        }
        
        $grpdetail = Invoke-Command @invoke | ConvertFrom-Json
    
        # Write the group details to a dedicated file
        $file = "./Security-Groups/$($group.Name).json"
        $grpdetail | ConvertTo-Json -depth 32 | Set-Content -Path $file
        Write-Output "`tWritten to [$((Get-ChildItem $file).FullName)]`r`n"
            
        # As long as the extracted catalog information is real, add it to the final list
        if ($group.Name.Length -gt 0)
        {
            $group.MemberCount = $grpdetail.members.Count
            $group.externalId = $grpdetail.externalId
            $groups += $group
        }
    }
}

# Store group list and return message to user
$file = "./Security-Groups.$dbxENV.json"
$groups | ConvertTo-Json -depth 32 | Set-Content -Path $file

Write-Output $linesep
Write-Output "$($groups.count) Security Groups written to [$((Get-ChildItem $file).FullName)]"
Write-Output $linesep
