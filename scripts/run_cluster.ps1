# PowerShell script to launch DFSha cluster locally or via Docker
param(
    [string]$Mode = "docker" # "docker" or "native"
)

$ErrorActionPreference = "Stop"

if ($Mode -eq "docker") {
    Write-Host "Starting DFSha cluster using Docker Compose..." -ForegroundColor Green
    docker compose up --build -d
    Write-Host "DFSha cluster started!" -ForegroundColor Green
    Write-Host "ControlNode Primary: http://localhost:8000"
    Write-Host "ControlNode Standby: http://localhost:8010"
    Write-Host "DataNode 1:         http://localhost:8001"
    Write-Host "DataNode 2:         http://localhost:8002"
    Write-Host "DataNode 3:         http://localhost:8003"
    Write-Host "`nTo open the interactive CLI run:" -ForegroundColor Yellow
    Write-Host "  docker compose run --rm client"
} else {
    Write-Host "Starting DFSha cluster in native background processes..." -ForegroundColor Green
    $VenvPy = "$PSScriptRoot\..\.venv\Scripts\python.exe"
    if (-not (Test-Path $VenvPy)) {
        $VenvPy = "python"
    }

    $Env:PYTHONPATH = "$PSScriptRoot\.."

    # Start Primary ControlNode
    $p1 = Start-Process -FilePath $VenvPy -ArgumentList "-m uvicorn controlnode.main:app --port 8000" -PassThru -NoNewWindow
    # Start Standby ControlNode
    $Env:DFSHA_ROLE = "standby"
    $Env:DFSHA_PRIMARY_URL = "http://localhost:8000"
    $p2 = Start-Process -FilePath $VenvPy -ArgumentList "-m uvicorn controlnode.main:app --port 8010" -PassThru -NoNewWindow

    # Start 3 DataNodes
    $Env:DFSHA_CONTROLNODE_URL = "http://localhost:8000"
    $Env:DFSHA_CONTROLNODE_STANDBY_URL = "http://localhost:8010"
    
    $Env:DFSHA_NODE_ID = "datanode-1"
    $Env:DFSHA_PORT = "8001"
    $Env:DFSHA_PUBLIC_URL = "http://localhost:8001"
    $p3 = Start-Process -FilePath $VenvPy -ArgumentList "-m uvicorn datanode.main:app --port 8001" -PassThru -NoNewWindow

    $Env:DFSHA_NODE_ID = "datanode-2"
    $Env:DFSHA_PORT = "8002"
    $Env:DFSHA_PUBLIC_URL = "http://localhost:8002"
    $p4 = Start-Process -FilePath $VenvPy -ArgumentList "-m uvicorn datanode.main:app --port 8002" -PassThru -NoNewWindow

    $Env:DFSHA_NODE_ID = "datanode-3"
    $Env:DFSHA_PORT = "8003"
    $Env:DFSHA_PUBLIC_URL = "http://localhost:8003"
    $p5 = Start-Process -FilePath $VenvPy -ArgumentList "-m uvicorn datanode.main:app --port 8003" -PassThru -NoNewWindow

    Write-Host "Native cluster launched with PIDs: $($p1.Id), $($p2.Id), $($p3.Id), $($p4.Id), $($p5.Id)" -ForegroundColor Green
}
