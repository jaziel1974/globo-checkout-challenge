param([string]$BaseUrl = 'http://localhost:8080')

$ErrorActionPreference = 'Stop'
$BaseUrl = $BaseUrl.TrimEnd('/')

function Invoke-ComposeCheckDocker([string[]]$DockerArguments) {
    $output = & docker @DockerArguments 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Docker verification failed: $($DockerArguments -join ' ')" }
    return ($output -join "`n").Trim()
}

function Wait-ComposeHttp([string]$Origin) {
    $deadline = [datetime]::UtcNow.AddSeconds(75)
    do {
        try {
            $response = Invoke-WebRequest -Uri "$Origin/v3/api-docs" -TimeoutSec 3 -SkipHttpErrorCheck
            if ([int]$response.StatusCode -eq 200) { return $response.Content | ConvertFrom-Json -AsHashtable }
        } catch { }
        Start-Sleep -Milliseconds 500
    } while ([datetime]::UtcNow -lt $deadline)
    throw "HTTP readiness failed for $Origin"
}

$secondContainerId = $null
$secondContainerName = 'globo-kafka-validation-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
try {
    Write-Output 'Building and starting the packaged applications.'
    $startup = Invoke-ComposeCheckDocker -DockerArguments @('compose', 'up', '--build', '-d')
    Write-Output $startup
    $api = Wait-ComposeHttp $BaseUrl
    $v2 = $api.paths['/v2/operations/checkout'].post
    if (-not $v2) { throw 'Generated OpenAPI omitted the v2 route.' }
    foreach ($status in @('200', '400', '404', '409', '422', '424')) {
        if (-not $v2.responses.ContainsKey($status)) { throw "Generated v2 OpenAPI omitted response $status" }
    }
    if ($v2.responses.ContainsKey('202')) { throw 'Generated OpenAPI unexpectedly exposes HTTP 202.' }
    if ($api.paths.ContainsKey('/v2/operations/checkout/{attemptId}')) { throw 'Generated OpenAPI unexpectedly exposes a status route.' }
    Write-Output 'Generated OpenAPI verified: v2 has direct 200/400/404/409/422/424 responses, without HTTP 202 or a status route.'

    foreach ($topic in @('checkout-v2-commands', 'checkout-v2-failures')) {
        $configuration = Invoke-ComposeCheckDocker -DockerArguments @('compose', 'exec', '-T', 'kafka',
            '/opt/kafka/bin/kafka-configs.sh', '--bootstrap-server', 'kafka:29092', '--entity-type', 'topics',
            '--entity-name', $topic, '--describe')
        foreach ($required in @('cleanup.policy=delete', 'retention.ms=604800000', 'retention.bytes=-1', 'min.insync.replicas=1')) {
            if (-not $configuration.Contains($required)) { throw "Topic $topic omitted effective configuration $required" }
        }
        Write-Output $configuration
    }

    Write-Output 'Starting an isolated second BFF for concurrent HTTP validation.'
    $createdOutput = Invoke-ComposeCheckDocker -DockerArguments @('compose', 'run', '--no-deps', '-d',
        '--publish', '127.0.0.1::8080', '--name', $secondContainerName, 'bff')
    $createdIds = @($createdOutput -split '\r?\n' | Where-Object { $_ -match '^[a-f0-9]{12,64}$' })
    if ($createdIds.Count -eq 1) { $secondContainerId = $createdIds[0] }
    else { $secondContainerId = Invoke-ComposeCheckDocker -DockerArguments @('inspect', '--format', '{{.Id}}', $secondContainerName) }
    if ($secondContainerId -notmatch '^[a-f0-9]{12,64}$') { throw 'Second BFF did not return an unambiguous container id.' }
    $binding = Invoke-ComposeCheckDocker -DockerArguments @('port', $secondContainerId, '8080/tcp')
    if ($binding -notmatch '^127\.0\.0\.1:(\d+)$') { throw 'Second BFF port binding is not local and unambiguous.' }
    $secondOrigin = 'http://127.0.0.1:' + $Matches[1]
    $null = Wait-ComposeHttp $secondOrigin
    & (Join-Path $PSScriptRoot 'smoke-kafka.ps1') -BaseUrl $BaseUrl -SecondBaseUrl $secondOrigin
    $null = Invoke-ComposeCheckDocker -DockerArguments @('rm', '-f', $secondContainerId)
    $secondContainerId = $null

    Write-Output 'Checking broker/Redis outages and automatic recovery.'
    & (Join-Path $PSScriptRoot 'smoke-kafka-outages.ps1') -BaseUrl $BaseUrl

    $serviceLogs = Invoke-ComposeCheckDocker -DockerArguments @('compose', 'logs', '--no-color', 'bff', 'worker')
    if ($serviceLogs -match '4111111111114242|4111111111110002|"cvv"\s*:\s*"?123|cvv=123') {
        throw 'Service logs contain a forbidden card fixture value; logs were not saved.'
    }
    Set-Content -LiteralPath (Join-Path (Split-Path $PSScriptRoot -Parent) 'validacao-kafka-compose-servicos.log') -Value $serviceLogs
    Write-Output 'Service logs inspected: full card/CVV fixtures absent.'
    Write-Output (Invoke-ComposeCheckDocker -DockerArguments @('compose', 'ps'))
    Write-Output 'Compose verification passed; local services remain running.'
} finally {
    if ($secondContainerId -and $secondContainerId -match '^[a-f0-9]{12,64}$') {
        $null = Invoke-ComposeCheckDocker -DockerArguments @('rm', '-f', $secondContainerId)
    }
}
