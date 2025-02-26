###############################################################################
# Get-Dbx-Workspaces
#
# Polls Databricks for all workspaces and the resulting definitions in a 
# JSON file
###############################################################################

# Command line arguments - first item is expected to be environment
$dbxEnv = "ACCOUNT"

# Get the workspaces in this Databricks account
$linesep = "======================================================================"
Write-Output $linesep
Write-Output "Getting WORKSPACE list (using Databricks profile ACCOUNT)"
Write-Output $linesep

$invoke = @{ 
    ScriptBlock = { databricks account workspaces list --profile $($args[0]) }
    ArgumentList = $dbxEnv
}

$wkspc = Invoke-Command @invoke

# Results returned from Databricks CLI are in the form:
# ########### <workspace-name>       STATE
# Need an iterator to use for picking first as ID and second as Name
$i = 0
# Setup final array to hold found ID / Name list
$fin = @()

# Loop over the workspaces and extract the ID & name (ignore state)
# Split on <space> gives an array of elements...
foreach ($outer in $wkspc) 
{
    $ws = [PSCustomObject]@{
        ID = 0
        Name = "TBD"
    }

    foreach ($w in $outer -split " ")
    {
        # many elements are blank, so screen these out
        if ($w -ne "")
        {
            # First non-blank element (index mod 3 = 0) is ID
            if($i % 3 -eq 0) 
            {
                $ws.ID = $w
            }
            # Second non-blank is Name
            if($i % 3 -eq 1) 
            {
                $ws.Name = $w
            }

            # iterate the position indicator
            $i++
        }
    }

    # Get the workspace definition and store it in a dedicated file
    Write-Output "- Getting workspace definition for $($ws.Name)"
    $invoke = @{ 
        ScriptBlock = { databricks account workspaces get $($args[1]) --profile $($args[0]) }
        ArgumentList = $dbxEnv, $ws.ID
    }
    $wsdetail = Invoke-Command @invoke | ConvertFrom-Json

    # Write the group details to a dedicated file
    $file = "./Workspaces/$($ws.Name).json"
    $wsdetail | ConvertTo-Json -depth 32 | Set-Content -Path $file
    Write-Output "`tWritten to [$((Get-ChildItem $file).FullName)]`r`n"


    # Add the extracted workspace to the final list
    $fin += $ws
}

# Store Workspaces & write message to user
$file = "./Workspaces.$dbxEnv.json"
$fin | ConvertTo-Json -depth 1 | Set-Content -Path $file

Write-Output $linesep
Write-Output "$($fin.count) Workspaces written to [$((Get-ChildItem $file).FullName)]"
Write-Output $linesep
