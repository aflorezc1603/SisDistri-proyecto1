# PowerShell script to execute test suite
$VenvPy = "$PSScriptRoot\..\.venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) {
    $VenvPy = "python"
}

$Env:PYTHONPATH = "$PSScriptRoot\.."
& $VenvPy -m pytest "$PSScriptRoot\..\tests\test_all.py" -v -s
