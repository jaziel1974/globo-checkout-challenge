param(
    [string]$BaseUrl = 'http://localhost:8080',
    [string]$SecondBaseUrl
)

$ErrorActionPreference = 'Stop'
$BaseUrl = $BaseUrl.TrimEnd('/')

function Assert-Smoke {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

function Invoke-CheckoutHttp {
    param([string]$Route, [hashtable]$Payload)
    $started = [System.Diagnostics.Stopwatch]::StartNew()
    $response = Invoke-WebRequest -Uri "$BaseUrl$Route" -Method Post -ContentType 'application/json' `
        -Body ($Payload | ConvertTo-Json -Depth 8 -Compress) -SkipHttpErrorCheck -TimeoutSec 10
    [pscustomobject]@{
        Status = [int]$response.StatusCode
        Body = $response.Content | ConvertFrom-Json
        Milliseconds = $started.ElapsedMilliseconds
    }
}

function New-SmokeCart {
    param([string[]]$Slugs = @('premium'))
    $response = Invoke-CheckoutHttp '/operations/pre-checkout' @{ products = @($Slugs | ForEach-Object { @{ slug = $_ } }) }
    Assert-Smoke ($response.Status -eq 200) 'Pre-checkout did not return HTTP 200.'
    Assert-Smoke (-not [string]::IsNullOrWhiteSpace($response.Body.cartId)) 'Pre-checkout omitted cartId.'
    return $response.Body
}

function New-CardSelection {
    param([string]$LastDigits)
    @{ name = 'Cartão de Crédito'; card = @{
        holderName = 'Ana Souza'; number = "411111111111$LastDigits"; expiry = '12/28'; cvv = '123'
    } }
}

function Assert-Approved {
    param($Response, $Cart, [bool]$Pix)
    Assert-Smoke ($Response.Status -eq 200) 'Expected approval HTTP 200.'
    $body = $Response.Body
    Assert-Smoke (($body.PSObject.Properties.Name | Sort-Object) -join ',' -eq 'cartId,paidAt,payment,products,status') 'Approval root JSON contract changed.'
    Assert-Smoke ($body.cartId -eq $Cart.cartId -and $body.status -eq 'APROVADO') 'Approval identifiers/status are incorrect.'
    Assert-Smoke (-not [string]::IsNullOrWhiteSpace($body.paidAt)) 'Approval omitted paidAt.'
    $total = [decimal]0
    foreach ($product in $Cart.products) { $total += [decimal]$product.price }
    Assert-Smoke ([decimal]$body.payment.amount -eq $total -and $body.payment.currency -eq 'BRL') 'Approval amount differs from pre-checkout snapshot.'
    if ($Pix) {
        Assert-Smoke (($body.payment.PSObject.Properties.Name | Sort-Object) -join ',' -eq 'amount,currency,method,pixCode') 'Pix payment JSON contract changed.'
        Assert-Smoke ($body.payment.pixCode -eq "PIX-SIMULADO-$($Cart.cartId)") 'Pix code differs from the original contract.'
    } else {
        Assert-Smoke (($body.payment.PSObject.Properties.Name | Sort-Object) -join ',' -eq 'amount,currency,lastDigits,method') 'Card payment JSON contract changed.'
        Assert-Smoke ($body.payment.lastDigits -eq '4242') 'Approval lastDigits are incorrect.'
    }
}

function Assert-OriginalError {
    param($Response, [int]$Status, [string]$ErrorCode)
    Assert-Smoke ($Response.Status -eq $Status -and $Response.Body.errorCode -eq $ErrorCode) 'Unexpected checkout error status/code.'
    Assert-Smoke (($Response.Body.PSObject.Properties.Name | Sort-Object) -join ',' -eq 'cartId,displayType,errorCode,frontendError') 'Error root JSON contract changed.'
    Assert-Smoke (-not [string]::IsNullOrWhiteSpace($Response.Body.frontendError.code)) 'Error omitted frontendError.code.'
}

$results = @()
$declineContracts = @()
foreach ($route in @('/operations/checkout', '/v2/operations/checkout')) {
    $cardCart = New-SmokeCart
    $cardRequest = @{ cartId = $cardCart.cartId; paymentMethod = (New-CardSelection '4242') }
    $approved = Invoke-CheckoutHttp $route $cardRequest
    Assert-Approved $approved $cardCart $false
    $duplicate = Invoke-CheckoutHttp $route $cardRequest
    Assert-OriginalError $duplicate 409 'PAYMENT_ALREADY_APPROVED'
    $results += [pscustomobject]@{ Route = $route; Scenario = 'card-approved-and-duplicate'; Status = $approved.Status; Milliseconds = $approved.Milliseconds }

    $retryCart = New-SmokeCart
    $declined = Invoke-CheckoutHttp $route @{ cartId = $retryCart.cartId; paymentMethod = (New-CardSelection '0002') }
    Assert-OriginalError $declined 422 'PAYMENT_DECLINED'
    $declined.Body.cartId = '<cartId>'
    $declineContracts += ($declined.Body | ConvertTo-Json -Depth 8 -Compress)
    $retry = Invoke-CheckoutHttp $route @{ cartId = $retryCart.cartId; paymentMethod = (New-CardSelection '4242') }
    Assert-Approved $retry $retryCart $false
    $results += [pscustomobject]@{ Route = $route; Scenario = 'declined-then-approved'; Status = $declined.Status; Milliseconds = $declined.Milliseconds }

    $pixCart = New-SmokeCart
    $pix = Invoke-CheckoutHttp $route @{ cartId = $pixCart.cartId; paymentMethod = @{ name = 'Pix' } }
    Assert-Approved $pix $pixCart $true
    $results += [pscustomobject]@{ Route = $route; Scenario = 'pix-approved'; Status = $pix.Status; Milliseconds = $pix.Milliseconds }

    $debitCart = New-SmokeCart -Slugs @('basico', 'premium', 'familia')
    $debitSelection = New-CardSelection '4242'
    $debitSelection.name = 'Cartão de Débito'
    $debit = Invoke-CheckoutHttp $route @{ cartId = $debitCart.cartId; paymentMethod = $debitSelection }
    Assert-Approved $debit $debitCart $false
    Assert-Smoke ($debit.Body.payment.method -eq 'Cartão de Débito') 'Debit payment method changed.'
    Assert-Smoke ($debit.Body.products.Count -eq 3) 'Multi-product snapshot was not preserved.'
    $results += [pscustomobject]@{ Route = $route; Scenario = 'debit-three-product-snapshot'; Status = $debit.Status; Milliseconds = $debit.Milliseconds }
}
Assert-Smoke ($declineContracts[0] -ceq $declineContracts[1]) 'v1/v2 decline JSON values differ.'
if ($SecondBaseUrl) {
    $concurrentCart = New-SmokeCart
    $payload = @{ cartId = $concurrentCart.cartId; paymentMethod = @{ name = 'Pix' } } | ConvertTo-Json -Depth 8 -Compress
    $http = [System.Net.Http.HttpClient]::new()
    $http.Timeout = [timespan]::FromSeconds(10)
    $requests = @()
    $responses = @()
    try {
        $pending = foreach ($origin in @($BaseUrl, $SecondBaseUrl.TrimEnd('/'))) {
            $request = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::Post, "$origin/v2/operations/checkout")
            $request.Content = [System.Net.Http.StringContent]::new($payload, [System.Text.Encoding]::UTF8, 'application/json')
            $requests += $request
            $http.SendAsync($request)
        }
        foreach ($task in $pending) { $responses += $task.GetAwaiter().GetResult() }
        Assert-Smoke (@($responses | Where-Object { [int]$_.StatusCode -eq 200 }).Count -eq 1) 'Concurrent v2 calls did not produce exactly one approval.'
        $conflicts = @($responses | Where-Object { [int]$_.StatusCode -eq 409 })
        Assert-Smoke ($conflicts.Count -eq 1) 'Concurrent v2 calls did not produce one conflict.'
        $conflict = $conflicts[0].Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
        Assert-Smoke ($conflict.errorCode -in @('PAYMENT_IN_PROGRESS', 'PAYMENT_ALREADY_APPROVED')) 'Unexpected v2 concurrency conflict.'
        $results += [pscustomobject]@{ Route = '/v2/operations/checkout'; Scenario = 'two-bff-instances-one-approval'; Status = '200/409'; Milliseconds = $null }
    } finally {
        foreach ($response in $responses) { $response.Dispose() }
        foreach ($request in $requests) { $request.Dispose() }
        $http.Dispose()
    }
}
$results | Format-Table -AutoSize
Write-Output 'Smoke passed: v1/v2 use separate carts; no cross-version safeguard is claimed.'
