param(
    [ValidateSet('build','run','smoke','measure')][string]$Action = 'build',
    [int]$Port = 0
)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$candidates = @()
if ($env:JAVA_HOME) { $candidates += $env:JAVA_HOME }
if (Test-Path -LiteralPath "$env:USERPROFILE/.jdks") {
    $candidates += Get-ChildItem -LiteralPath "$env:USERPROFILE/.jdks" -Directory | Sort-Object Name -Descending | Select-Object -ExpandProperty FullName
}
$javacCommand = Get-Command javac -ErrorAction SilentlyContinue
if ($javacCommand) { $candidates += Split-Path (Split-Path $javacCommand.Source) }
$selectedJdk = $null
foreach ($candidate in $candidates) {
    $javacPath = Join-Path $candidate 'bin/javac.exe'
    if (Test-Path -LiteralPath $javacPath) {
        $versionText = & $javacPath -version 2>&1 | Out-String
        if ($versionText -match 'javac (\d+)' -and [int]$Matches[1] -ge 21) { $selectedJdk = $candidate; break }
    }
}
if (-not $selectedJdk) { throw 'JDK 21+ required. Set JAVA_HOME to a JDK installation.' }
$env:JAVA_HOME = $selectedJdk
$env:PATH = (Join-Path $selectedJdk 'bin') + [IO.Path]::PathSeparator + $env:PATH
Write-Host "Using JDK: $selectedJdk"
if ($Action -eq 'build') {
    & "$PSScriptRoot/mvnw.cmd" -B verify
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    exit 0
}
if (-not (Test-Path 'target/pulsestream-1.0.0.jar')) { throw 'Run ./dev.ps1 build first.' }
if ($Action -eq 'run') {
    if ($Port -eq 0) { $Port = 9092 }
    & java -jar target/pulsestream-1.0.0.jar $Port
} else {
    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    $pythonPath = if ($pythonCommand) { $pythonCommand.Source } else { $null }
    $bundledPython = Join-Path $env:USERPROFILE '.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
    if (Test-Path -LiteralPath $bundledPython) { $pythonPath = $bundledPython }
    if (-not $pythonPath) { throw 'Python 3.10+ required for tools.' }
    if ($Action -eq 'smoke') { & $pythonPath tools/smoke.py --java (Join-Path $selectedJdk 'bin/java.exe') }
    else { & $pythonPath tools/measure.py --java (Join-Path $selectedJdk 'bin/java.exe') }
}
if ($LASTEXITCODE -ne 0) { throw "Action failed: $Action" }
