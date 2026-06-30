<#
.SYNOPSIS
  One-command QTP demo: bring the stack up, wait until healthy, run the end-to-end
  compliance demonstration, and open the evidence pack.

.DESCRIPTION
  This is the single entry point for a live presentation on Windows:

      ./scripts/demo.ps1

  It is idempotent — safe to run repeatedly. Any extra arguments are passed straight
  through to `qtp demo run` (e.g. ./scripts/demo.ps1 --inject-critical).

.EXAMPLE
  ./scripts/demo.ps1
.EXAMPLE
  ./scripts/demo.ps1 --inject-critical --shift A
#>
[CmdletBinding()]
param(
    [switch]$NoUp,                      # skip `docker compose up` (stack already running)
    [int]$TimeoutSeconds = 120,        # how long to wait for required services
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$DemoArgs                 # forwarded to `qtp demo run`
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

function Write-Step($msg) { Write-Host "`n>>> $msg" -ForegroundColor Cyan }

# 1) Bring the stack up ------------------------------------------------------
if (-not $NoUp) {
    Write-Step "Starting the QTP stack (docker compose up -d)..."
    docker compose up -d
}

# 2) Wait until required services are healthy --------------------------------
Write-Step "Waiting for services to become healthy (preflight)..."
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
do {
    qtp preflight run | Out-Null
    $ready = ($LASTEXITCODE -eq 0)
    if (-not $ready) {
        if ((Get-Date) -gt $deadline) {
            Write-Host "Required services did not come up within $TimeoutSeconds s." -ForegroundColor Red
            qtp preflight run
            exit 1
        }
        Start-Sleep -Seconds 4
    }
} until ($ready)

# 3) Run the demonstration ---------------------------------------------------
Write-Step "Running the single-command compliance demonstration..."
qtp demo run @DemoArgs
$demoExit = $LASTEXITCODE

# 4) Open the newest evidence pack -------------------------------------------
$pack = Get-ChildItem -Path (Join-Path $repo 'evidence') -Filter 'pack-*' -Directory -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($pack) {
    $html = Join-Path $pack.FullName 'evidence.html'
    if (Test-Path $html) {
        Write-Step "Opening evidence pack: $html"
        Start-Process $html
    }
}

exit $demoExit
