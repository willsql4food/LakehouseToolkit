# Repository: bi-poc-ssis

## Purpose
A Proof of Concept repository for working with SSIS packages, typically in an on-premises environment

## Branches
### Permanent
__dev__ - This branch contains the source code that is deployed to a Development environment (ex. FD-DCSQLDEV1).  It serves as the environment for:
* Integration testing - do changes to a given package work for the entire project?
* Performance evaluation - do the packages perform as expected when dealing with data of expected size?

__qa__ - This branch contains ___Release Candidate___ code.
* This branch contains code which is development complete and ready for integration testing by a QA team
* Deployments to QA environment happen from this branch

__prod__ - This branch contains ___production ready___ code.
* Only packages with approved changes are merged into this branch
* Deployments to prod (FD-DCSQLPROD) happen from this branch


### Temporary
__BUG...__ - _Bug Fix_ branches are short lived.  They are typically derived from the __dev__ branch and used for the duration of fixing a known issue, though sometimes __prod__ is the source.

___Naming___: `BUG_<Ticket#>_<Short description>`  
ex: _BUG_6019_Store_Hierarchy_

---

__ft...__ - Feature branches exist on the days to weeks scale.  They typically map to a given user story.  They are for:
* Adding a defined feature
* Enhancing an existing set of functionality
* Building a new capability

___Naming___: `ft_<Ticket#>_<Short description>`  
ex: _ft_6129_Add_Report_Branding_

---

For BUG or Feature branches, follow these steps:
1. Create a branch for the bug or feature
1. Make changes to satisfy the need
1. Unit test in the branch itself
1. Submit a pull request for the _bug..._ or _ft_..._ branch --> __dev__
1. Pending approval & merge conflicts, the changes are merged into __dev__ 
1. The _bug..._ or _ft_..._ branch is deleted

Once the changes have been tested in __dev__ they are ready for merge to __prod__ and deployment 

## Process Example 
``` mermaid
%%{init: {
    'themeVariables':{
        'git0': '#4444ff',
        'gitInv0': '#4444ff',
        'git1': '#ffff00',
        'gitInv1': '#ffff00',
        'git2': '#00aa00',
        'gitInv2': '#00aa00',
        'git3': '#aa00aa',
        'gitInv3': '#aa00aa',
        'git4': '#ff00ff',
        'gitInv4': '#ff00ff',
        'git5': '#ff0000',
        'gitInv5': '#ff0000',
        'git6': '#880088',
        'gitInv6': '#880088',

        'commitLabelFontSize': '14px',
        'tagLabelFontSize': '14px',
        'tagLabelBackground': '#ffff22',
        'tagLabelColor': '#0000ff'
    },
    'gitGraph': {
        'showBranches': false, 
        'mainBranchName': 'main', 
        'mainBranchOrder': 3}
        }
    }%%

gitGraph
    commit id:"DEV Branch" type: Highlight
    branch ft4 order: 4
    commit id:"ft_AddStore" type: highlight
    commit id:"Build"

    checkout main
    merge ft4  tag: "RC 1.0"

    branch qa order: 2
    commit id: "QA Branch" type: Highlight

    checkout main
    branch ft5 order: 5
    commit id:"ft_PLR_enhance" type: highlight
    commit id:"MoreData"

    checkout qa
    commit id: "Test 1.0"

    checkout ft5
    commit id:"Report"

    checkout main
    merge ft5
    checkout qa
    merge main

    commit id:"Test PLR v1"
    checkout ft5
    commit id:"Theme"
    commit id:"Drillthrough"
    checkout main
    merge ft5 tag: "RC 1.1"

    checkout qa
    merge main
    commit id: "Test 1.1"

    branch prod order: 1
    commit id:"PROD Branch" tag: "v 1.1" type: Highlight
    commit id:"Field"

    checkout main
    commit id: " "
    branch ft6 order: 6
    commit id:"ft_POS_Enhance" type: highlight

    checkout qa
    merge main
    commit id:"Test POS"

    branch bg1 order: 5
    commit id:"BUG_POS_Count" type: highlight
    commit id:"Fix POS"

    checkout main
    merge bg1
    commit id:"Bug fix to DEV"

    checkout qa
    merge main
    commit id:"Test BugFix"

    checkout prod
    merge qa
    commit id:"Apply BugFix"

    checkout ft6
    merge main
    commit id:"Get bug fix"
    commit id:"Quick work"
    checkout main
    merge ft6
    commit id:"Work continues..."
```
