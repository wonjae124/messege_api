Write-Host "Starting K6 Test..."
k6 run -e RPS=300 -e DURATION=20s ./docs/performance/k6_load_test.js | Out-Null
Write-Host "K6 Test Finished. Measuring PENDING drain time..."

$startTime = Get-Date
while ($true) {
    $countStr = docker exec postgres-perf psql -U alarmuser -d alarmdb -t -c "SELECT count(*) FROM alarm_outbox_events WHERE status = 'PENDING';"
    if (-not [string]::IsNullOrWhiteSpace($countStr)) {
        $count = [int]($countStr.Trim())
        if ($count -eq 0) {
            $endTime = Get-Date
            $elapsed = ($endTime - $startTime).TotalSeconds
            Write-Host "PENDING completely drained in $elapsed seconds after K6 test."
            break
        }
    }
    Start-Sleep -Milliseconds 500
}

Write-Host "Getting Circuit Breaker stats from Prometheus..."
$response = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/query?query=resilience4j_circuitbreaker_calls_total{kind='not_permitted'}"
if ($response.data.result.Length -gt 0) {
    $cbStats = $response.data.result[0].value[1]
    Write-Host "Circuit Breaker Blocked Count: $cbStats"
} else {
    Write-Host "Circuit Breaker Blocked Count: 0"
}
