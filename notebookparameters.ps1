$RootPath = "C:\Users\arun.chand\Downloads\HCL\Dev-UAT"

# Define replacement mappings
$Replacements = @(
    @{
        "old_lakehouse" = '"default_lakehouse": "07d66a6b-6006-4d50-af17-c3eeb1a862b6"'
        "old_name" = '"default_lakehouse_name": "LHDigitalAnalyticsSilver"'
        "old_workspace" = '"default_lakehouse_workspace_id": "85a32c46-45bf-453a-9d3b-2c35a5f14bf7"'
        "new_lakehouse" = '"default_lakehouse": "fc3c21c6-7add-41d2-9ea0-1aa5748785cf"'
        "new_name" = '"default_lakehouse_name": "LHDigitalAnalyticsSilver"'
        "new_workspace" = '"default_lakehouse_workspace_id": "2007cd51-09cc-49b4-aaa5-ca5e2f8f3a64"'
    },
    @{
        "old_lakehouse" = '"default_lakehouse": "52c24218-5198-45c3-b36e-78d6c0d6e186"'
        "old_name" = '"default_lakehouse_name": "LHDev"'
        "old_workspace" = '"default_lakehouse_workspace_id": "85a32c46-45bf-453a-9d3b-2c35a5f14bf7"'
        "new_lakehouse" = '"default_lakehouse": "5ebdd465-3724-4edb-8a29-2cc233fe96e7"'
        "new_name" = '"default_lakehouse_name": "LHDev"'
        "new_workspace" = '"default_lakehouse_workspace_id": "2007cd51-09cc-49b4-aaa5-ca5e2f8f3a64"'
    },
    @{
        "old_lakehouse" = '"default_lakehouse": "f83fbd59-f899-4dee-a930-c1b83d7b14dd"'
        "old_name" = '"default_lakehouse_name": "LHDigitalAnalytics"'
        "old_workspace" = '"default_lakehouse_workspace_id": "85a32c46-45bf-453a-9d3b-2c35a5f14bf7"'
        "new_lakehouse" = '"default_lakehouse": "cb78832c-69c0-4de2-98dd-5607b0612845"'
        "new_name" = '"default_lakehouse_name": "LHDigitalAnalytics"'
        "new_workspace" = '"default_lakehouse_workspace_id": "2007cd51-09cc-49b4-aaa5-ca5e2f8f3a64"'
    },
    @{
        "old_lakehouse" = '"default_lakehouse": "2ad7827c-d97b-4b65-bb00-4bb537924aa3"'
        "old_name" = '"default_lakehouse_name": "LHDigitalAnalyticsGold"'
        "old_workspace" = '"default_lakehouse_workspace_id": "85a32c46-45bf-453a-9d3b-2c35a5f14bf7"'
        "new_lakehouse" = '"default_lakehouse": "aef52d0e-fb1a-4c75-b15c-37f2acf4e6ac"'
        "new_name" = '"default_lakehouse_name": "LHDigitalAnalyticsGold"'
        "new_workspace" = '"default_lakehouse_workspace_id": "2007cd51-09cc-49b4-aaa5-ca5e2f8f3a64"'
    }
)

# Get all 'notebook-content.py' files in all subdirectories
$Files = Get-ChildItem -Path $RootPath -Recurse -Filter "notebook-content.py"

foreach ($File in $Files) {
    # Read file content
    $Content = Get-Content -Path $File.FullName -Raw

    # Perform replacements only if all three conditions match
    foreach ($Replacement in $Replacements) {
        if ($Content -match [regex]::Escape($Replacement["old_lakehouse"]) -and
            $Content -match [regex]::Escape($Replacement["old_name"]) -and
            $Content -match [regex]::Escape($Replacement["old_workspace"])) {

            # Replace the values
            $Content = $Content -replace [regex]::Escape($Replacement["old_lakehouse"]), $Replacement["new_lakehouse"]
            $Content = $Content -replace [regex]::Escape($Replacement["old_name"]), $Replacement["new_name"]
            $Content = $Content -replace [regex]::Escape($Replacement["old_workspace"]), $Replacement["new_workspace"]

            Write-Host "Updated: $($File.FullName)"
        }
    }

    # Save the updated content back to the file
    Set-Content -Path $File.FullName -Value $Content -Encoding UTF8
}

Write-Host "All matching notebook-content.py files updated successfully."
