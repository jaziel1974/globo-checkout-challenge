param(
    [ValidatePattern('^[A-Za-z0-9_.,]+$')]
    [string]$TestClasses,
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$Module = 'checkout-worker',
    [switch]$RecoverDocker,
    [switch]$FullSuite,
    [switch]$EnvironmentOnly
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
if (-not $FullSuite -and -not $EnvironmentOnly -and [string]::IsNullOrWhiteSpace($TestClasses)) {
    throw 'Provide -TestClasses for a focused gate or -FullSuite for clean verify.'
}
if ($FullSuite -and $TestClasses) {
    throw '-FullSuite and -TestClasses cannot be combined.'
}

function Invoke-ServiceCommand {
    param([string]$FilePath, [string[]]$NativeArguments, [int]$TimeoutSeconds = 15)
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $NativeArguments) { $startInfo.ArgumentList.Add($argument) }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        $process.Kill()
        return [pscustomobject]@{ ExitCode = -1; Output = ''; Error = 'Service command timed out' }
    }
    $result = [pscustomobject]@{
        ExitCode = $process.ExitCode
        Output = $stdout.GetAwaiter().GetResult().Trim()
        Error = $stderr.GetAwaiter().GetResult().Trim()
    }
    $process.Dispose()
    return $result
}

$docker = Invoke-ServiceCommand 'docker.exe' @('version', '--format', '{{.Server.Version}}')
if ($docker.ExitCode -ne 0) {
    if (-not $RecoverDocker) { throw "Docker is unavailable: $($docker.Error)" }
    Write-Output 'Recovering Docker Desktop after failed backend check.'
    $dockerDirectory = Join-Path $env:ProgramFiles 'Docker\Docker'
    $shutdown = Invoke-ServiceCommand (Join-Path $dockerDirectory 'DockerCli.exe') @('-Shutdown') 10
    $terminate = Invoke-ServiceCommand 'wsl.exe' @('--terminate', 'docker-desktop') 15
    if ($terminate.ExitCode -ne 0) { throw "Could not terminate Docker distribution: $($terminate.Error)" }
    $shutdownDeadline = [datetime]::UtcNow.AddSeconds(30)
    while ((Get-Process 'Docker Desktop' -ErrorAction SilentlyContinue) -and [datetime]::UtcNow -lt $shutdownDeadline) {
        Start-Sleep -Seconds 1
    }
    Start-Process -FilePath (Join-Path $dockerDirectory 'Docker Desktop.exe') -WindowStyle Hidden
    $startupDeadline = [datetime]::UtcNow.AddMinutes(5)
    $nextStartupUpdate = [datetime]::UtcNow.AddSeconds(30)
    do {
        Start-Sleep -Seconds 2
        if (-not (Get-Process 'Docker Desktop' -ErrorAction SilentlyContinue)) {
            Start-Process -FilePath (Join-Path $dockerDirectory 'Docker Desktop.exe') -WindowStyle Hidden
        }
        $docker = Invoke-ServiceCommand 'docker.exe' @('version', '--format', '{{.Server.Version}}') 10
        if ($docker.ExitCode -ne 0 -and [datetime]::UtcNow -ge $nextStartupUpdate) {
            Write-Output 'Waiting for Docker backend readiness.'
            $nextStartupUpdate = [datetime]::UtcNow.AddSeconds(30)
        }
    } while ($docker.ExitCode -ne 0 -and [datetime]::UtcNow -lt $startupDeadline)
    if ($docker.ExitCode -ne 0) { throw "Docker did not recover: $($docker.Error)" }
}
if ($EnvironmentOnly) {
    Write-Output "Docker server ready: $($docker.Output). No Maven command was run."
    return
}
Write-Output "Docker server ready: $($docker.Output). Starting tests immediately."

$runStarted = [datetime]::UtcNow
$mavenArguments = if ($FullSuite) { @('clean', 'verify') } else {
    @('-pl', $Module, '-am', "-Dtest=$TestClasses", '-Dsurefire.failIfNoSpecifiedTests=false', 'test')
}
Push-Location $projectRoot
try {
    & (Join-Path $projectRoot 'mvnw.cmd') @mavenArguments
    if ($LASTEXITCODE -ne 0) { throw "Maven failed with exit code $LASTEXITCODE" }
    $reports = Get-ChildItem -Path (Join-Path $projectRoot '*\target\surefire-reports\TEST-*.xml') |
        Where-Object { $_.LastWriteTimeUtc -ge $runStarted.AddSeconds(-1) }
    if (-not $reports) { throw 'No fresh test reports were generated.' }
    $tests = 0; $failures = 0; $errors = 0; $skips = 0
    $executed = @()
    foreach ($reportFile in $reports) {
        [xml]$report = Get-Content -Raw -LiteralPath $reportFile.FullName
        $tests += [int]$report.testsuite.tests
        $failures += [int]$report.testsuite.failures
        $errors += [int]$report.testsuite.errors
        $skips += [int]$report.testsuite.skipped
        $executed += ($report.testsuite.name -split '\.')[-1]
    }
    foreach ($expected in @($TestClasses -split ',' | Where-Object { $_ })) {
        if ($expected -notin $executed) { throw "Missing test report: $expected" }
    }
    Write-Output "Verified reports: tests=$tests failures=$failures errors=$errors skipped=$skips"
    if ($failures -gt 0 -or $errors -gt 0 -or $skips -gt 0) {
        throw 'Stage validation failed: failures, errors or skipped tests were found.'
    }
} finally {
    Pop-Location
}
