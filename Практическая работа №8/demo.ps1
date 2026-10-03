# Демонстрация ПР №8 — микросервисная система поставщиков и доставки.
#
#   .\demo.ps1                — сквозной сценарий: заказ → Supplier → Kafka → Delivery → SSE,
#                               трассировка по X-Request-ID, идемпотентность, ошибки
#   .\demo.ps1 -Failover      — Supplier Service остановлен: retry, circuit breaker, восстановление
#   .\demo.ps1 -Hang          — Supplier Service «завис» (docker compose pause): таймаут вместо ожидания
#   .\demo.ps1 -DeliveryDown  — Delivery Service остановлен: событие ждёт в Kafka и обрабатывается позже
#
# Если скрипты запрещены: powershell -ExecutionPolicy Bypass -File .\demo.ps1

param(
    [string]$BaseUrl = "http://localhost:8000",
    [switch]$Failover,
    [switch]$Hang,
    [switch]$DeliveryDown
)

[Console]::OutputEncoding = [Text.Encoding]::UTF8
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot   # docker compose ищет compose.yaml в текущей папке

$OrderBody = '{"customerName": "Петрова Анна", "deliveryAddress": "г. Москва, Ленинский проспект, д. 30к2, кв. 117", "comment": "Домофон не работает", "items": [{"productId": 5, "quantity": 1}, {"productId": 4, "quantity": 1}]}'

# docker compose пишет ход выполнения в stderr: в PowerShell 5.1 при ErrorActionPreference=Stop
# это считалось бы ошибкой, поэтому вызовы compose идут через обёртку
function Invoke-Compose {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker compose @args 2>&1 |
            ForEach-Object { if ($_ -is [Management.Automation.ErrorRecord]) { $_.Exception.Message } else { "$_" } } |
            Where-Object { $_ -and $_ -ne 'System.Management.Automation.RemoteException' }
    } finally {
        $ErrorActionPreference = $previous
    }
}

function Write-Title([string]$text) {
    Write-Host ""
    Write-Host "=== $text ===" -ForegroundColor Cyan
}

function New-RequestId([string]$prefix) {
    return $prefix + "-" + (-join ((48..57) + (97..102) | Get-Random -Count 6 | ForEach-Object { [char]$_ }))
}

# HTTP через curl.exe: тело уходит в UTF-8 (Invoke-RestMethod в PowerShell 5.1 портит кириллицу),
# возвращаются код ответа, время, X-Request-ID и JSON
function Invoke-Api([string]$Method, [string]$Path, [string]$Body, [string]$RequestId, [switch]$Quiet) {
    $bodyFile = [IO.Path]::GetTempFileName()
    $headersFile = [IO.Path]::GetTempFileName()
    $curlArgs = @('-s', '-X', $Method, '-o', $bodyFile, '-D', $headersFile, '-w', '%{http_code} %{time_total}', "$BaseUrl$Path")
    if ($RequestId) { $curlArgs += @('-H', "X-Request-ID: $RequestId") }
    $requestFile = $null
    if ($Body) {
        $requestFile = [IO.Path]::GetTempFileName()
        [IO.File]::WriteAllText($requestFile, $Body, (New-Object Text.UTF8Encoding $false))
        $curlArgs += @('-H', 'Content-Type: application/json', '--data-binary', "@$requestFile")
    }
    $code, $time = (& curl.exe @curlArgs) -split ' '
    $text = [IO.File]::ReadAllText($bodyFile, [Text.Encoding]::UTF8)
    $header = Select-String -Path $headersFile -Pattern '^X-Request-Id:\s*(\S+)' | Select-Object -First 1
    Remove-Item $bodyFile, $headersFile
    if ($requestFile) { Remove-Item $requestFile }

    $result = [pscustomobject]@{
        Status    = [int]$code
        Seconds   = [double]::Parse($time, [Globalization.CultureInfo]::InvariantCulture)
        RequestId = if ($header) { $header.Matches[0].Groups[1].Value } else { $RequestId }
        Body      = if ($text.Trim()) { $text | ConvertFrom-Json } else { $null }
    }
    if (-not $Quiet) {
        $color = if ($result.Status -lt 400) { 'Green' } else { 'Yellow' }
        Write-Host ("{0} {1} -> HTTP {2} за {3:N3} с, X-Request-ID: {4}" -f $Method, $Path, $result.Status, $result.Seconds, $result.RequestId) -ForegroundColor $color
    }
    return $result
}

function Show-Health {
    $health = (Invoke-Api GET "/actuator/health/system" -Quiet).Body
    Write-Host ("Система: {0}" -f $health.status)
    $health.components.PSObject.Properties | ForEach-Object {
        $d = $_.Value.details
        [pscustomobject]@{
            'Сервис'          = $_.Name
            'Статус'          = $_.Value.status
            'БД'              = if ($d.db) { $d.db } else { $d.r2dbc }
            'Kafka'           = $d.kafka
            'Circuit breaker' = $d.circuitBreakers
            'Ответ, мс'       = $d.responseTimeMs
            'Ошибка'          = $d.error
        }
    } | Format-Table -AutoSize | Out-Host
}

# SSE-поток статусов заказа: каждая строка с местным временем получения
function Watch-Order([long]$OrderId) {
    curl.exe -sN "$BaseUrl/api/orders/$OrderId/events" | ForEach-Object {
        if ($_) { Write-Host ("[{0:HH:mm:ss.fff}] {1}" -f (Get-Date), $_) }
    }
    Write-Host "Сервер закрыл поток после конечного статуса" -ForegroundColor Green
}

function Wait-Healthy([string]$Service) {
    Write-Host "Ждём, пока $Service пройдёт healthcheck..." -NoNewline
    for ($i = 0; $i -lt 60; $i++) {
        $status = docker inspect -f '{{.State.Health.Status}}' "msa-delivery-$Service-1"
        if ($status -eq 'healthy') { Write-Host " healthy" -ForegroundColor Green; return }
        Start-Sleep -Seconds 2
        Write-Host "." -NoNewline
    }
    Write-Host " не дождались" -ForegroundColor Red
}

function Send-Orders([int]$Count, [string]$Prefix) {
    for ($i = 1; $i -le $Count; $i++) {
        $r = Invoke-Api POST "/api/orders" $OrderBody (New-RequestId $Prefix) -Quiet
        $color = if ($r.Status -lt 400) { 'Green' } else { 'Yellow' }
        $detail = if ($r.Body.detail) { $r.Body.detail } else { "заказ $($r.Body.id): $($r.Body.status)" }
        Write-Host ("{0}. POST /api/orders -> HTTP {1} за {2,6:N3} с  {3}" -f $i, $r.Status, $r.Seconds, $detail) -ForegroundColor $color
    }
}

# Трассировка: строки всех контейнеров с этим X-Request-ID, по времени Docker (одни часы для всех)
function Show-Trace([string]$RequestId) {
    Invoke-Compose logs --no-color --timestamps --since 15m |
        Select-String -SimpleMatch $RequestId |
        ForEach-Object {
            $service, $rest = $_.Line -split '\s*\|\s', 2
            $stamp, $text = $rest -split ' ', 2
            [pscustomobject]@{ Stamp = $stamp; Service = $service.Trim(); Text = $text }
        } |
        Sort-Object Stamp |
        ForEach-Object { Write-Host ("{0,-19}| {1}" -f $_.Service, $_.Text) }
}

# ---------------------------------------------------------------------------------------------
if ($Failover) {
    Write-Title "Отказ Supplier Service: docker compose stop supplier-service"
    Invoke-Compose stop supplier-service | Out-Null
    Show-Health

    Write-Title "Заказы при недоступном Supplier Service: retry → circuit breaker → быстрый отказ"
    Send-Orders 5 "fail"

    Write-Title "Остальная система работает: заказы читаются, каталог — быстрый 503 от API Gateway"
    Invoke-Api GET "/api/orders" -RequestId (New-RequestId "fail") | Out-Null
    (Invoke-Api GET "/api/products" -RequestId (New-RequestId "fail")).Body | Format-List title, status, detail | Out-Host
    Show-Health

    Write-Title "Восстановление: docker compose start supplier-service"
    Invoke-Compose start supplier-service | Out-Null
    Wait-Healthy "supplier-service"
    Write-Host "Ждём, пока circuit breaker перейдёт в HALF_OPEN (до 15 с после открытия)..."
    for ($i = 0; $i -lt 20; $i++) {
        $cb = (Invoke-Api GET "/actuator/health/system" -Quiet).Body.components.orderService.details.circuitBreakers
        if ($cb -notmatch 'OPEN' -or $cb -match 'HALF_OPEN') { break }
        Start-Sleep -Seconds 1
    }
    Send-Orders 2 "recover"
    Show-Health
    return
}

if ($Hang) {
    Write-Title "Supplier Service «завис»: docker compose pause supplier-service"
    Invoke-Compose pause supplier-service | Out-Null
    Write-Host "Контейнер заморожен: TCP-соединение устанавливается, но ответа нет"
    Send-Orders 3 "hang"

    Write-Title "docker compose unpause supplier-service"
    Invoke-Compose unpause supplier-service | Out-Null
    Write-Host "Ждём 16 с: circuit breaker перейдёт в HALF_OPEN и пропустит пробные запросы..."
    Start-Sleep -Seconds 16
    Send-Orders 2 "recover"
    return
}

if ($DeliveryDown) {
    Write-Title "Delivery Service остановлен: docker compose stop delivery-service"
    Invoke-Compose stop delivery-service | Out-Null
    $order = Invoke-Api POST "/api/orders" $OrderBody (New-RequestId "async")
    $id = $order.Body.id
    Write-Host "Заказ $id создан со статусом $($order.Body.status) — Order Service не ждёт Delivery Service"
    Start-Sleep -Seconds 5
    Write-Host ("Через 5 с статус заказа {0}: {1} — событие OrderCreated ждёт в Kafka" -f $id, (Invoke-Api GET "/api/orders/$id" -Quiet).Body.status)

    Write-Title "Группа delivery-service в Kafka: LAG > 0 — есть непрочитанное событие"
    Invoke-Compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group delivery-service

    Write-Title "docker compose start delivery-service — событие обрабатывается после запуска"
    Invoke-Compose start delivery-service | Out-Null
    Watch-Order $id
    return
}

# ---------------------------------------------------------------------------------------------
Write-Title "0) Контейнеры системы"
docker compose ps --format "table {{.Name}}\t{{.Status}}"

Write-Title "1) Состояние сервисов: GET /actuator/health/system"
Show-Health

Write-Title "2) Каталог Supplier Service: GET /api/products (Client → Traefik → API Gateway → Supplier)"
$products = (Invoke-Api GET "/api/products" -RequestId (New-RequestId "demo")).Body
$products | Format-Table id, name, @{ n = 'supplier'; e = { $_.supplier.name } }, price, stock -AutoSize | Out-Host

Write-Title "3) Создание заказа: Order Service → Supplier Service (проверка товаров) → CONFIRMED → OrderCreated в Kafka"
$requestId = New-RequestId "demo"
$order = Invoke-Api POST "/api/orders" $OrderBody $requestId
$id = $order.Body.id
$order.Body | Format-List id, status, customerName, deliveryAddress, total | Out-Host
$order.Body.items | Format-Table productId, productName, supplierName, price, quantity, sum -AutoSize | Out-Host

Write-Title "4) SSE: GET /api/orders/$id/events — статусы заказа приходят по мере изменения (Flux)"
Watch-Order $id

Write-Title "5) Итог: заказ, доставка, остаток на складе"
(Invoke-Api GET "/api/orders/$id").Body | Format-List id, status, deliveryId, courier, total | Out-Host
(Invoke-Api GET "/api/deliveries?orderId=$id").Body | Format-Table id, orderId, status, courier, price, recipient -AutoSize | Out-Host
$coffee = (Invoke-Api GET "/api/products/5" -Quiet).Body
Write-Host ("Остаток «{0}»: {1} шт. (Supplier Service списал товар по событию OrderCreated)" -f $coffee.name, $coffee.stock)

Write-Title "6) Идемпотентность: то же событие OrderCreated (тот же eventId) отправлено в Kafka повторно"
(Invoke-Api POST "/api/orders/$id/republish").Body | Format-List | Out-Host
Start-Sleep -Seconds 3
$deliveries = @((Invoke-Api GET "/api/deliveries?orderId=$id" -Quiet).Body)
Write-Host ("Доставок у заказа {0}: {1}; остаток «{2}»: {3} шт. — повтор ничего не изменил" -f $id, $deliveries.Count, $coffee.name, (Invoke-Api GET "/api/products/5" -Quiet).Body.stock) -ForegroundColor Green
Invoke-Compose logs --no-color --since 20s delivery-service supplier-service | Select-String "Повторное событие"

Write-Title "7) Трассировка запроса $requestId по логам всех контейнеров"
Show-Trace $requestId

Write-Title "8) Ошибки: нет в наличии (409), нет в каталоге (422), невалидный запрос (400)"
$noStock = '{"customerName": "Иванов Иван", "deliveryAddress": "г. Москва, Тверская, д. 12, кв. 45", "items": [{"productId": 6, "quantity": 1}]}'
$unknown = '{"customerName": "Иванов Иван", "deliveryAddress": "г. Москва, Тверская, д. 12, кв. 45", "items": [{"productId": 99, "quantity": 1}]}'
$invalid = '{"customerName": "", "items": [{"productId": 1, "quantity": 0}]}'
foreach ($body in @($noStock, $unknown, $invalid)) {
    (Invoke-Api POST "/api/orders" $body (New-RequestId "demo")).Body | Format-List status, detail, orderId, orderStatus | Out-Host
}
