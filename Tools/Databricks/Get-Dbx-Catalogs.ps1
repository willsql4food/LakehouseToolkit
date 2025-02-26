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

# Get the catalogs in this Databricks account
$linesep = "======================================================================"
Write-Output $linesep
Write-Output "Getting CATALOG list (using Databricks profile $dbxEnv)"
Write-Output $linesep

$invoke = @{ 
    ScriptBlock = { databricks catalogs list --profile $($args[0]) }
    ArgumentList = $dbxEnv
}
$catlist = Invoke-Command @invoke

# Results returned from Databricks CLI are in the form:
# <catalog-name>   <Type>  Comment

# Setup final array to hold catalog information list
$cats = @()

# Loop over the catalogs and extract the name, type, and comment
# Split on <space> gives an array of elements, 
# but comment is sliced up and will have to be reassembled
$i = 0
# while ($i -le 4 )
while ($i -le $catlist.count)
{
    # The result has a header line, so ignore first 'catalog' returned
    $i++

    # New object to hold this catalog
    $cat = [PSCustomObject]@{
        Name = ""
        Type = ""
        Comment = ""
        bindings = @()
        privilege_assignments = @()
        schemas = @()
    }
    
    # Read the current line and a provisional concatenation of the next line
    $catline = $catlist[$i]
    $next = "~$($catlist[$i + 1])"

    # If the next line is not a new _CATALOG...
    while (-not $next.Contains('_CATALOG') -and $i -lt $catlist.count)
    {
        # Append to current line and iterate
        $catline += $next
        $i++
        $next = "~$($catlist[$i + 1])"
    }

    # Split the outer catalog by spaces and iterate over the parts
    $j = 0
    foreach ($element in $catline -split " ")
    {
        # many elements are blank, so screen these out
        if ($element -ne "")
        {
            # First non-blank element (index mod 3 = 0) is Name
            if($j -eq 0) 
            {
                $cat.Name = $element
            }
            # Second non-blank is Type
            if($j -eq 1) 
            {
                $cat.Type = $element
            }
            # Subsequent non-blank values are pieces of Comment
            if($j -gt 1) 
            {
                $cat.Comment += "$element ".Replace('~', "`r`n")
            }
            # Only iterate if we found an element
            $j++
        }
    }

    # Get various attribute arrays for the catalog
    if ($cat.Name -ne "")
    {
        #########################################
        # Workspace bindings
        #########################################
        Write-Output "$($cat.Name)"
        Write-Output "`t- Getting workspace bindings"
        $invoke = @{ 
            ScriptBlock = { databricks workspace-bindings get-bindings catalog $($args[1]) --profile $($args[0]) }
            ArgumentList = $dbxEnv, $cat.Name
        }
        $bind = Invoke-Command @invoke | ConvertFrom-Json
    
        # If there are any workspace bindings...
        if ($bind)
        {
            $cat.bindings = $bind.bindings
        }
    
        #########################################
        # Granted permissions
        #########################################
        Write-Output "`t- Getting permission grants"
        $invoke = @{ 
            ScriptBlock = { databricks grants get catalog $($args[1]) --profile $($args[0]) }
            ArgumentList = $dbxEnv, $cat.Name
        }
        $grants = Invoke-Command @invoke | ConvertFrom-Json
    
        # If this catalog has any bindings, collect them
        if ($grants)
        {
            $cat.privilege_assignments = $grants.privilege_assignments
        }
    
        #########################################
        # Schemas
        #########################################
        Write-Output "`t- Getting schemas"
        $invoke = @{ 
            ScriptBlock = { databricks schemas list $($args[1]) --profile $($args[0]) }
            ArgumentList = $dbxEnv, $cat.Name
        }
        $schemas = Invoke-Command @invoke

        # If this catalog has any schemas, collect them
        if ($schemas.count -gt 1)
        {
            # Ignore the first line (header)
            $schemas = $schemas[1..($schemas.count-1)]

            # Loop through each schema
            foreach ($s in $schemas)
            {
                # New object to hold schema attributes
                $sch = [PSCustomObject]@{
                    full_name = ""
                    owner = ""
                    comment = ""
                }

                # Delimited string, but may have multiple blanks
                $schemaline = $s.Split(" ")
                $j = 0
                # Loop through elements
                foreach ($l in $schemaline)
                {
                    if ($l -ne " ")
                    {
                        # Full name is first actual string
                        if ($j -eq 0)
                        {
                            $sch.full_name = $l
                            $j++
                        }

                        # Owner is second actual string
                        if ($j -eq 1)
                        {
                            $sch.owner = $l
                            $j++
                        }

                        # Remainder is comment
                        if ($j -gt 1)
                        {
                            $sch.comment += "$l "
                        }
                    }
                }
                # Trim trailing space from comment
                $sch.comment = $sch.comment.Trim()

                # Add this schema to the catalog's schemas array
                $cat.schemas += $sch
            }
        }
    
        # As long as the extracted catalog information is real, add it to the final list
        if ($cat.Name.Length -gt 0)
        {
            $cats += $cat
        }
    }
}

# Store Catalogs and return message to user
$file = "./Catalogs.$($dbxEnv).json"
$cats | ConvertTo-Json -depth 32 | Set-Content -Path $file

Write-Output $linesep
Write-Output "$($cats.count) Catalogs written to [$((Get-ChildItem $file).FullName)]"
Write-Output $linesep
