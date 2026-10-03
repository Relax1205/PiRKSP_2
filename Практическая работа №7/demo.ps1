# Демонстрация реактивного HTTP API Delivery Service (Spring WebFlux).
#
#   .\demo.ps1               — все сценарии: Mono, Flux, ошибки, WebClient, SSE
#   .\demo.ps1 -OrderId 5    — создать доставку для другого заказа
#
# Если скрипты запрещены: powershell -ExecutionPolicy Bypass -File .\demo.ps1

param(
    [string]$BaseUrl = "http://localhost:8090",
    [long]$OrderId = 1
)

[Console]::OutputEncoding = [Text.Encoding]::UTF8
$ErrorActionPreference = "Stop"
$api = "$BaseUrl/api/deliveries"

function Write-Title([string]$text) {
    Write-Host ""
    Write-Host "=== $text ===" -ForegroundColor Cyan
}

# Запрос через curl.exe: тело отправляется в UTF-8 (Invoke-RestMethod в PowerShell 5.1 портит кириллицу)
function Invoke-Api([string]$Method, [string]$Url, [string]$Body, [switch]$Quiet) {
    $curlArgs = @('-s', '-X', $Method, '-w', '\n%{http_code}', $Url)
    $tmp = $null
    if ($Body) {
        $tmp = [IO.Path]::GetTempFileName()
        [IO.File]::WriteAllText($tmp, $Body, (New-Object Text.UTF8Encoding $false))
        $curlArgs += @('-H', 'Content-Type: application/json', '--data-binary', "@$tmp")
    }
    $lines = @(& curl.exe @curlArgs)
    if ($tmp) { Remove-Item $tmp }
    $code = [int]$lines[-1]
    $json = ($lines | Select-Object -SkipLast 1) -join "`n"
    if (-not $Quiet) {
        $color = if ($code -lt 400) { 'Green' } else { 'Yellow' }
        Write-Host "$Method $Url -> HTTP $code" -ForegroundColor $color
    }
    if ($json.Trim()) {
        return $json | ConvertFrom-Json
    }
}

# Повторный запуск: удаляем доставку, созданную прошлым прогоном
$old = Invoke-Api GET $api -Quiet | Where-Object { $_.orderId -eq $OrderId }
if ($old) {
    Invoke-Api DELETE "$api/$($old.id)" -Quiet
}

Write-Title "1) GET /api/deliveries — несколько объектов: Flux<Delivery> -> JSON-массив"
Invoke-Api GET $api | Format-Table id, orderId, recipient, courier, price, status -AutoSize | Out-Host

Write-Title "2) GET /api/deliveries?status=IN_TRANSIT — фильтр (оператор filter)"
Invoke-Api GET "$api`?status=IN_TRANSIT" | Format-Table id, orderId, recipient, courier, status -AutoSize | Out-Host

Write-Title "3) GET /api/deliveries/1 — один объект: Mono<Delivery> (аналог RSocket Request-Response)"
Invoke-Api GET "$api/1" | Format-List | Out-Host

Write-Title "4) GET /api/deliveries/999 — нет данных: switchIfEmpty -> 404 Problem Details"
Invoke-Api GET "$api/999" | Format-List title, status, detail | Out-Host

Write-Title "5) POST /api/deliveries — WebClient: Delivery Service -> Order Service -> новая доставка"
$created = Invoke-Api POST $api "{`"orderId`": $OrderId}"
$created | Format-List | Out-Host
$id = $created.id

Write-Title "6) POST для несуществующего и отменённого заказа — ошибки WebClient в реактивной цепочке"
Invoke-Api POST $api '{"orderId": 999}' | Format-List status, detail | Out-Host
Invoke-Api POST $api '{"orderId": 6}' | Format-List status, detail | Out-Host

Write-Title "7) PUT /api/deliveries/$id — запрещённый переход CREATED -> DELIVERED"
Invoke-Api PUT "$api/$id" '{"courier": "Орлов Игорь", "price": 199, "status": "DELIVERED"}' |
        Format-List status, detail | Out-Host

Write-Title "8) GET /api/deliveries/$id/details — доставка + заказ из Order Service (flatMap + WebClient)"
$details = Invoke-Api GET "$api/$id/details"
$details.order | Format-List id, status, comment, total | Out-Host
$details.order.items | Format-Table productName, price, quantity, sum -AutoSize | Out-Host

Write-Title "9) GET /api/deliveries/$id/stream — SSE: Flux событий (аналог RSocket Request-Stream)"
Write-Host "Через 2 с запускается курьер (POST /simulate). События приходят по мере смены статуса:" -ForegroundColor Yellow
$job = Start-Job -ArgumentList "$api/$id/simulate" -ScriptBlock {
    param($url)
    Start-Sleep -Seconds 2
    curl.exe -s -X POST $url | Out-Null
}
curl.exe -sN "$api/$id/stream" | ForEach-Object {
    if ($_) { Write-Host ("[{0:HH:mm:ss}] {1}" -f (Get-Date), $_) }
}
Remove-Job $job -Force
Write-Host "Сервер закрыл поток после финального статуса DELIVERED" -ForegroundColor Green

Write-Title "10) DELETE /api/deliveries/$id"
Invoke-Api DELETE "$api/$id"
Invoke-Api GET "$api/$id" | Format-List status, detail | Out-Host
