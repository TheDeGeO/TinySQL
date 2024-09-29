function Execute-MyQuery {
    param (
        [Parameter(Mandatory = $true)]
        [string]$queryFilePath,
        [Parameter(Mandatory = $true)]
        [int]$port,
        [Parameter(Mandatory = $true)]
        [string]$ip
    )

    # Read the contents of the text file
    $queryCommands = Get-Content $queryFilePath -Raw

    # Split the contents into individual commands using semicolon as the delimiter
    $commands = $queryCommands -split ';'

    # Execute each command as a query
    foreach ($command in $commands) {
        # Trim any leading or trailing whitespace from the command
        $command = $command.Trim()

        # Skip empty commands
        if ([string]::IsNullOrWhiteSpace($command)) {
            continue
        }

        # Your code logic to execute the query goes here
        # Example: Print the query
        Write-Host "Executing query: $command"
    }
}