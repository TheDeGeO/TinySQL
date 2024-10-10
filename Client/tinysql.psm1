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

    # Start measuring total execution time
    $totalStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    # Execute each command as a query
    foreach ($command in $commands) {
        # Trim any leading or trailing whitespace from the command
        $command = $command.Trim()

        # Skip empty commands
        if ([string]::IsNullOrWhiteSpace($command)) {
            continue
        }

        # Start measuring individual command execution time
        $commandStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

        # Use the Send-SQLCommand function from tinysqlclient.ps1
        # Pass the $ip and $port parameters to the function
        . $PSScriptRoot\tinysqlclient.ps1 -IP $ip -Port $port
        Send-SQLCommand -command $command

        # Stop measuring individual command execution time
        $commandStopwatch.Stop()
        Write-Host "Command execution time: $($commandStopwatch.Elapsed.TotalMilliseconds) ms"
    }

    # Stop measuring total execution time
    $totalStopwatch.Stop()
    Write-Host "Total execution time: $($totalStopwatch.Elapsed.TotalMilliseconds) ms"
}