
$ErrorActionPreference = "Stop"

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$ports = @(8000, 8001, 8002, 8003)
$processes = @()

if (-not (Test-Path $python)) {
    throw "Python virtual environment not found: $python"
}

try {
    foreach ($port in $ports) {
        $arguments = @(
            "-m", "cli.main", "start",
            "--host", "127.0.0.1",
            "--port", "$port",
            "--demo-tasks"
        )

        if ($port -eq 8000) {
            $arguments += "--web"
        }
        else {
            $arguments += @("--seed", "127.0.0.1:8000")
        }

        Write-Host "Starting node-$port..." -ForegroundColor Cyan

        $process = Start-Process `
            -FilePath $python `
            -ArgumentList $arguments `
            -WorkingDirectory $PSScriptRoot `
            -WindowStyle Hidden `
            -PassThru

        $processes += $process
        Start-Sleep -Seconds 2

        $process.Refresh()

        if ($process.HasExited) {
            throw "Node on port $port exited during startup."
        }
    }

    Write-Host ""
    Write-Host "All four nodes started." -ForegroundColor Green
    Write-Host "Dashboard: http://127.0.0.1:8765/"
    Write-Host "Demo tasks: add, multiply"
    Write-Host "Press Ctrl+C to stop the nodes."

    while ($true) {
        $running = @(
            $processes | Where-Object { -not $_.HasExited }
        )

        if ($running.Count -eq 0) {
            throw "All node processes have exited."
        }

        Start-Sleep -Seconds 1
    }
}
finally {
    Write-Host "`nStopping launched nodes..." -ForegroundColor Yellow

    foreach ($process in $processes) {
        try {
            $process.Refresh()

            if (-not $process.HasExited) {
                Stop-Process -Id $process.Id -Force `
                    -ErrorAction SilentlyContinue

                Write-Host "Stopped process $($process.Id)"
            }
        }
        catch {
            # The process may already have exited.
        }
    }

    Write-Host "Cleanup finished." -ForegroundColor Green
}
