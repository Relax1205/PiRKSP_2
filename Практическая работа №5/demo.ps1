# Демонстрация балансировки нагрузки Traefik и отказоустойчивости Delivery Service.
#
#   .\demo.ps1                 — 9 запросов подряд: видно, что отвечают разные экземпляры
#   .\demo.ps1 -Failover       — плюс остановка одного экземпляра и повторная проверка
#
# Если скрипты запрещены: powershell -ExecutionPolicy Bypass -File .\demo.ps1 -Failover

param(
    [string]$BaseUrl = "http://localhost",
    [int]$Requests = 9,
    [switch]$Failover
)

[Console]::OutputEncoding = [Text.Encoding]::UTF8
$ErrorActionPreference = "Stop"
$project = "baas-delivery"

# hostname контейнера = короткий CONTAINER ID -> находим имя контейнера
function Get-ContainerNames {
    $map = @{}
    docker ps --filter "name=$project-delivery-service" --format "{{.ID}} {{.Names}}" | ForEach-Object {
        $id, $name = $_ -split " ", 2
        $map[$id] = $name
    }
    return $map
}

function Invoke-Series([string]$title) {
    Write-Host ""
    Write-Host "=== $title ===" -ForegroundColor Cyan
    $names = Get-ContainerNames
    $stats = @{}
    for ($i = 1; $i -le $Requests; $i++) {
        try {
            $r = Invoke-RestMethod -Uri "$BaseUrl/api/deliveries/instance" -TimeoutSec 5
            $name = $names[$r.hostname]
            if (-not $name) { $name = "?" }
            Write-Host ("{0,2}. 200  hostname={1}  instanceId={2}  container={3}" -f $i, $r.hostname, $r.instanceId, $name)
            $stats[$name] = 1 + [int]$stats[$name]
        } catch {
            Write-Host ("{0,2}. ОШИБКА: {1}" -f $i, $_.Exception.Message) -ForegroundColor Red
            $stats["ERROR"] = 1 + [int]$stats["ERROR"]
        }
    }
    Write-Host "Распределение запросов:" -ForegroundColor Yellow
    $stats.GetEnumerator() | Sort-Object Name | ForEach-Object { Write-Host ("  {0,-40} {1}" -f $_.Name, $_.Value) }
}

Write-Host "Экземпляры Delivery Service:" -ForegroundColor Yellow
docker ps --filter "name=$project-delivery-service" --format "table {{.ID}}`t{{.Names}}`t{{.Status}}"

Invoke-Series "Балансировка: $Requests запросов GET /api/deliveries/instance через Traefik"

if ($Failover) {
    $victim = "$project-delivery-service-1"
    Write-Host ""
    Write-Host ">>> docker stop $victim" -ForegroundColor Magenta
    docker stop $victim | Out-Null

    Write-Host "Экземпляры Delivery Service после остановки:" -ForegroundColor Yellow
    docker ps -a --filter "name=$project-delivery-service" --format "table {{.ID}}`t{{.Names}}`t{{.Status}}"

    Invoke-Series "Один экземпляр остановлен — доступ через Traefik сохраняется"

    Write-Host ""
    Write-Host ">>> docker start $victim" -ForegroundColor Magenta
    docker start $victim | Out-Null
    Write-Host "Экземпляр запущен; после прохождения healthcheck Traefik вернёт его в балансировку."
}
