param(
    [string]$CoreUrl = "http://127.0.0.1:8787",
    [string]$EnvironmentFile = (Join-Path $PSScriptRoot "..\.env")
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

function Invoke-TCloudJson {
    param([string]$Method, [string]$Path, [object]$Body, [string]$Token)
    $headers = @{}
    if ($Token) { $headers.Authorization = "Bearer $Token" }
    $parameters = @{
        Uri = "$CoreUrl$Path"
        Method = $Method
        Headers = $headers
        ContentType = "application/json"
    }
    if ($null -ne $Body) { $parameters.Body = ($Body | ConvertTo-Json -Compress) }
    Invoke-RestMethod @parameters
}

function Get-HttpStatus {
    param([string]$Path, [string]$Token)
    try {
        $headers = @{}
        if ($Token) { $headers.Authorization = "Bearer $Token" }
        (Invoke-WebRequest -Uri "$CoreUrl$Path" -Headers $headers -UseBasicParsing -ErrorAction Stop).StatusCode
    } catch {
        [int]$_.Exception.Response.StatusCode
    }
}

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

$databaseUrl = (Get-Content -LiteralPath $EnvironmentFile | Where-Object { $_ -match '^TCLOUD_DATABASE_URL=' } | Select-Object -First 1).Split('=', 2)[1]
$databaseUrl = $databaseUrl -replace '(@(?:127\.0\.0\.1|localhost)):54329/', '$1:5432/'
$uri = [Uri]$databaseUrl
$env:PGPASSWORD = [Uri]::UnescapeDataString($uri.UserInfo.Split(':', 2)[1])
$databaseUser = $uri.UserInfo.Split(':', 2)[0]
$databaseName = $uri.AbsolutePath.TrimStart('/')
$psqlCommand = Get-Command psql.exe -ErrorAction SilentlyContinue
$psql = if ($psqlCommand) {
    $psqlCommand.Source
} else {
    Get-ChildItem 'C:\Program Files\PostgreSQL' -Filter psql.exe -Recurse -ErrorAction Stop |
        Where-Object { $_.FullName -match '\\bin\\psql\.exe$' } |
        Select-Object -First 1 -ExpandProperty FullName
}
if (-not $psql) { throw 'psql.exe nao encontrado.' }

$accountA = $null
$accountB = $null
$result = $null
try {
    Assert-True ((Get-HttpStatus -Path '/api/v1/files') -eq 401) 'Rota protegida aceitou requisicao sem token.'

    $accountA = Invoke-TCloudJson -Method POST -Path '/api/v1/onboarding/register' -Body @{ deviceName='E2E A'; platform='test'; appVersion='e2e' }
    $accountB = Invoke-TCloudJson -Method POST -Path '/api/v1/onboarding/register' -Body @{ deviceName='E2E B'; platform='test'; appVersion='e2e' }
    $fileA = [Guid]::NewGuid().ToString()
    $fileB = [Guid]::NewGuid().ToString()
    $suffix = [Guid]::NewGuid().ToString('N')
    $nameA = "isolation-a-$suffix.txt"
    $nameB = "isolation-b-$suffix.txt"
    $sql = @"
INSERT INTO telegram_index_files(id,canonical_id,user_id,telegram_peer_id,telegram_message_id,name,mime,size_bytes)
VALUES ('$fileA','$fileA','$($accountA.userId)',-91001,-91001,'$nameA','text/plain',1),
       ('$fileB','$fileB','$($accountB.userId)',-91002,-91002,'$nameB','text/plain',1);
"@
    & $psql -h $uri.Host -p $uri.Port -U $databaseUser -d $databaseName -v ON_ERROR_STOP=1 -q -c $sql
    if ($LASTEXITCODE -ne 0) { throw 'Falha ao preparar dados isolados.' }

    $filesA = Invoke-TCloudJson -Method GET -Path '/api/v1/files' -Token $accountA.token
    $filesB = Invoke-TCloudJson -Method GET -Path '/api/v1/files' -Token $accountB.token
    $jsonA = $filesA | ConvertTo-Json -Depth 8 -Compress
    $jsonB = $filesB | ConvertTo-Json -Depth 8 -Compress
    Assert-True ($jsonA.Contains($nameA) -and -not $jsonA.Contains($nameB)) 'Conta A recebeu dados de outra conta.'
    Assert-True ($jsonB.Contains($nameB) -and -not $jsonB.Contains($nameA)) 'Conta B recebeu dados de outra conta.'

    $pairing = Invoke-TCloudJson -Method POST -Path '/api/v1/devices/pairing' -Token $accountA.token -Body @{ deviceName='E2E pareado'; platform='test'; appVersion='e2e' }
    $paired = Invoke-TCloudJson -Method POST -Path '/api/v1/device-auth/exchange' -Body @{ code=$pairing.code; deviceName='E2E pareado'; platform='test'; appVersion='e2e' }
    Assert-True ((Get-HttpStatus -Path '/api/v1/files' -Token $paired.token) -eq 200) 'Dispositivo pareado nao acessou a propria conta.'
    Assert-True ((Get-HttpStatus -Path '/api/v1/device-auth/exchange' -Token '') -ne 200) 'Verificacao invalida do codigo reutilizado.'
    try {
        Invoke-TCloudJson -Method POST -Path '/api/v1/device-auth/exchange' -Body @{ code=$pairing.code; deviceName='reuso'; platform='test' } | Out-Null
        throw 'Codigo de pareamento foi aceito duas vezes.'
    } catch {
        if ($_.Exception.Message -eq 'Codigo de pareamento foi aceito duas vezes.') { throw }
    }

    Invoke-TCloudJson -Method POST -Path '/api/v1/devices/revoke' -Token $accountA.token -Body @{ deviceId=$paired.deviceId } | Out-Null
    Assert-True ((Get-HttpStatus -Path '/api/v1/files' -Token $paired.token) -eq 401) 'Token revogado continuou autorizado.'

    $result = [pscustomobject]@{
        unauthenticated = 'blocked'
        accountIsolation = 'passed'
        oneTimePairing = 'passed'
        revocation = 'passed'
        cleanup = 'pending'
    }
} finally {
    if ($accountA -or $accountB) {
        $ids = @($accountA.userId, $accountB.userId) | Where-Object { $_ }
        if ($ids.Count -gt 0) {
            $quoted = ($ids | ForEach-Object { "'$_'" }) -join ','
            & $psql -h $uri.Host -p $uri.Port -U $databaseUser -d $databaseName -q -c "DELETE FROM users WHERE id IN ($quoted);" | Out-Null
        }
    }
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}
$result.cleanup = 'completed'
$result
