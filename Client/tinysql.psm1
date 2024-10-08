function Invoke-MyQuery {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Query,
        [Parameter(Mandatory = $true)]
        [int]$port,
        [Parameter(Mandatory = $true)]
        [string]$ip
    )

    # Read the contents of the text file
    $queryCommands = Get-Content $Query -Raw

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

        # Use the Send-SQLCommand function from tinysqlclient.ps1
        # Pass the $ip and $port parameters to the function
        . $PSScriptRoot\tinysqlclient.ps1 -IP $ip -Port $port
        Send-SQLCommand -command $command
    }
}