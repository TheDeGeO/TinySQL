# Parameters for the TinySQL server and number of entries
param (
    [string]$IP,
    [int]$Port,
    [Parameter(Mandatory = $true)]
    [int]$NumEntries
)

# Import the TinySQL client functions
. .\tinysqlclient.ps1

# Function to generate a random name
function Get-RandomName {
    $names = @("John", "Jane", "Doe", "Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace",
               "Henry", "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter", "Quinn")
    return $names | Get-Random
}

# Function to generate a random address
function Get-RandomAddress {
    $streets = @("Main", "Elm", "Oak", "Pine", "Maple", "Cedar", "Birch", "Walnut", "Cherry", "Spruce",
                 "Ash", "Beech", "Cypress", "Fir", "Hickory", "Juniper", "Linden", "Magnolia", "Poplar", "Willow")
    $number = Get-Random -Minimum 100 -Maximum 999
    $street = $streets | Get-Random
    return "$number $street St"
}

# Create the database
$dbName = "test$NumEntries"
Send-SQLCommand -command "CREATE DATABASE $dbName"

# Set the current database
Send-SQLCommand -command "SET DATABASE $dbName"

# Create the table
$tableName = "table$NumEntries"
$createTableCommand = @"
CREATE TABLE $tableName (
    ID INT,
    NAME VARCHAR(50),
    AGE INT,
    ADDRESS VARCHAR(100),
    SALARY FLOAT
)
"@
Send-SQLCommand -command $createTableCommand

# Insert the specified number of records
for ($i = 1; $i -le $NumEntries; $i++) {
    $name = Get-RandomName
    $age = Get-Random -Minimum 18 -Maximum 80
    $address = Get-RandomAddress
    $salary = [math]::Round((Get-Random -Minimum 30000 -Maximum 150000) + (Get-Random -Maximum 1), 2)
    
    $insertCommand = "INSERT INTO $tableName VALUES ($i, '$name', $age, '$address', $salary);"
    Send-SQLCommand -command $insertCommand

    if ($i % 100 -eq 0) {
        Write-Host "Inserted $i records..."
    }
}

Write-Host "Database '$dbName' and table '$tableName' with $NumEntries entries have been created successfully."