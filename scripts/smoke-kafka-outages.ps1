param([string]$BaseUrl = 'http://localhost:8080')

$ErrorActionPreference = 'Stop'
$BaseUrl = $BaseUrl.TrimEnd('/')

function Assert-Outage([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Invoke-OutageHttp([string]$Route, [hashtable]$Payload) {
    $clock = [System.Diagnostics.Stopwatch]::StartNew()
    $response = Invoke-WebRequest -Uri "$BaseUrl$Route" -Method Post -ContentType 'application/json' `
        -Body ($Payload | ConvertTo-Json -Depth 8 -Compress) -SkipHttpErrorCheck -TimeoutSec 10
    [pscustomobject]@{ Status = [int]$response.StatusCode; Body = $response.Content | ConvertFrom-Json; Milliseconds = $clock.ElapsedMilliseconds }
}

function New-OutageCart {
    $response = Invoke-OutageHttp '/operations/pre-checkout' @{ products = @(@{ slug = 'premium' }) }
    Assert-Outage ($response.Status -eq 200) 'Pre-checkout failed before outage.'
    return [string]$response.Body.cartId
}

function Invoke-OutageDocker([string[]]$DockerArguments) {
    $output = & docker @DockerArguments 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Docker outage command failed: $($DockerArguments -join ' ')" }
    return ($output -join "`n").Trim()
}

$kafkaPaused = $false
$redisPaused = $false
$results = @()
try {
    $kafkaCart = New-OutageCart
    $v1Cart = New-OutageCart
    $null = Invoke-OutageDocker -DockerArguments @('compose', 'pause', 'kafka')
    $kafkaPaused = $true
    $pending = Invoke-OutageHttp '/v2/operations/checkout' @{ cartId = $kafkaCart; paymentMethod = @{ name = 'Pix' } }
    Assert-Outage ($pending.Status -eq 424 -and $pending.Body.errorCode -eq 'DEPENDENCY_UNAVAILABLE') 'Kafka outage did not produce standardized 424.'
    Assert-Outage ($pending.Body.frontendError.title -eq 'Resultado ainda não confirmado') 'Kafka outage incorrectly claimed a definitive failure.'
    # One second is allowed only for HTTP transmission and scheduling on this local machine.
    Assert-Outage ($pending.Milliseconds -le 6000) 'Kafka outage exceeded total five-second processing budget plus one-second HTTP tolerance.'
    $conflict = Invoke-OutageHttp '/v2/operations/checkout' @{ cartId = $kafkaCart; paymentMethod = @{ name = 'Pix' } }
    Assert-Outage ($conflict.Status -eq 409 -and $conflict.Body.errorCode -eq 'PAYMENT_IN_PROGRESS') 'Pending publication did not retain the original attempt.'
    $v1 = Invoke-OutageHttp '/operations/checkout' @{ cartId = $v1Cart; paymentMethod = @{ name = 'Pix' } }
    Assert-Outage ($v1.Status -eq 200) 'Kafka outage affected synchronous v1 checkout.'
    $results += [pscustomobject]@{ Scenario = 'kafka-paused'; Status = '424/409; v1=200'; Milliseconds = $pending.Milliseconds }
    $null = Invoke-OutageDocker -DockerArguments @('compose', 'unpause', 'kafka')
    $kafkaPaused = $false

    # Observe the existing internal attempt; never create a replacement HTTP operation to inspect it.
    $recoveryDeadline = [datetime]::UtcNow.AddSeconds(70)
    do {
        $approvedAttempt = Invoke-OutageDocker -DockerArguments @('compose', 'exec', '-T', 'redis', 'redis-cli', '--raw', 'HGET', "checkout:v2:cart:$kafkaCart", 'approvedAttemptId')
        if ($approvedAttempt) { break }
        Start-Sleep -Seconds 1
    } while ([datetime]::UtcNow -lt $recoveryDeadline)
    Assert-Outage (-not [string]::IsNullOrWhiteSpace($approvedAttempt)) 'Original pending attempt did not recover after Kafka resumed.'
    $publicationCount = Invoke-OutageDocker -DockerArguments @('compose', 'exec', '-T', 'redis', 'redis-cli', '--raw', 'HGET', "checkout:v2:attempt:$approvedAttempt", 'publicationCount')
    Assert-Outage ([int]$publicationCount -ge 1 -and [int]$publicationCount -le 3) 'Recovery exceeded publication budget.'
    $afterRecovery = Invoke-OutageHttp '/v2/operations/checkout' @{ cartId = $kafkaCart; paymentMethod = @{ name = 'Pix' } }
    Assert-Outage ($afterRecovery.Status -eq 409 -and $afterRecovery.Body.errorCode -eq 'PAYMENT_ALREADY_APPROVED') 'Recovered approval allowed another payment.'
    $results += [pscustomobject]@{ Scenario = 'same-attempt-recovered'; Status = '409 already approved'; Milliseconds = $afterRecovery.Milliseconds }

    $redisCart = New-OutageCart
    $null = Invoke-OutageDocker -DockerArguments @('compose', 'pause', 'redis')
    $redisPaused = $true
    $unavailable = Invoke-OutageHttp '/v2/operations/checkout' @{ cartId = $redisCart; paymentMethod = @{ name = 'Pix' } }
    Assert-Outage ($unavailable.Status -eq 424 -and $unavailable.Body.errorCode -eq 'DEPENDENCY_UNAVAILABLE') 'Redis outage did not produce standardized 424.'
    Assert-Outage ($unavailable.Milliseconds -le 6000) 'Redis outage exceeded total five-second processing budget plus one-second HTTP tolerance.'
    $results += [pscustomobject]@{ Scenario = 'redis-paused'; Status = 424; Milliseconds = $unavailable.Milliseconds }
    $null = Invoke-OutageDocker -DockerArguments @('compose', 'unpause', 'redis')
    $redisPaused = $false
    Start-Sleep -Milliseconds 500
    $unexpectedState = Invoke-OutageDocker -DockerArguments @('compose', 'exec', '-T', 'redis', 'redis-cli', '--raw', 'EXISTS', "checkout:v2:cart:$redisCart")
    Assert-Outage ($unexpectedState -eq '0') 'Timed-out cart lookup later created a v2 attempt.'
    $healthy = Invoke-OutageHttp '/v2/operations/checkout' @{ cartId = $redisCart; paymentMethod = @{ name = 'Pix' } }
    Assert-Outage ($healthy.Status -eq 200) 'Redis recovery did not restore normal v2 checkout.'
} finally {
    if ($redisPaused) { $null = Invoke-OutageDocker -DockerArguments @('compose', 'unpause', 'redis') }
    if ($kafkaPaused) { $null = Invoke-OutageDocker -DockerArguments @('compose', 'unpause', 'kafka') }
}

$results | Format-Table -AutoSize
Write-Output 'Outage smoke passed; original service processes resumed, no volumes removed.'
