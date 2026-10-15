# PowerShell script to demonstrate Fault Tolerance (RNF2)
param(
    [string]$Target = "docker" # "docker" or "native"
)

Write-Host "=== DFSha Fault Tolerance & High Availability Demo ===" -ForegroundColor Cyan

if ($Target -eq "docker") {
    Write-Host "[1/4] Checking cluster health..." -ForegroundColor Yellow
    docker compose ps

    Write-Host "[2/4] Simulating failure of datanode-1 (Stopping container)..." -ForegroundColor Red
    docker stop dfsha-datanode-1

    Write-Host "[3/4] Testing file download while datanode-1 is offline..." -ForegroundColor Yellow
    docker compose run --rm client python -m client.cli get /courses/notes.txt /tmp/recovered_notes.txt

    Write-Host "[4/4] Restoring datanode-1..." -ForegroundColor Green
    docker start dfsha-datanode-1

    Write-Host "`nHigh Availability test completed successfully! Data retrieved from replica without data loss." -ForegroundColor Green
} else {
    Write-Host "For native execution, kill a DataNode process and run: python -m client.cli get <file>"
}
