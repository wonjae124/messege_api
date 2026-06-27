param(
    [string]$mdFile = "docs/performance/PERFORMANCE_RPS_TABLE.md"
)

Write-Host "======================================="
Write-Host "🚀 Starting Automated RPS Table Extraction..."
Write-Host "======================================="

if (-not (Test-Path $mdFile)) {
    $header = @"
# RPS 단계별 부하 테스트 결과

| 시나리오 | Target RPS | Actual TPS | 총 요청 수 | API 성공률 | 평균 응답시간 | p95 응답시간 | 테스트 직후 큐 상태(Kafka/DB) | Drain |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
"@
    Set-Content -Path $mdFile -Value $header -Encoding UTF8
}

function Get-KafkaLag() {
    $cmd = 'docker exec kafka-perf kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group alarm_api_consumer_group 2> $null'
    try {
        $output = Invoke-Expression $cmd
        $totalLag = 0
        $hasData = $false
        foreach ($line in $output) {
            $parts = $line.Trim() -split '\s+'
            if ($parts.Count -ge 6 -and [int]::TryParse($parts[5], [ref]$null)) {
                $totalLag += [int]$parts[5]
                $hasData = $true
            }
        }
        if ($hasData) { return $totalLag }
    } catch {}
    return 0
}

function Get-DBLag() {
    $cmd = 'docker exec postgres-perf psql -U alarmuser -d alarmdb -t -c "SELECT count(*) FROM alarms WHERE status = ''PENDING'' OR status = ''SENDING'' OR status = ''RETRY_WAIT'';" 2> $null'
    try {
        $output = Invoke-Expression $cmd
        $val = $output.Trim()
        if ([int]::TryParse($val, [ref]$null)) {
            return [int]$val
        }
    } catch {}
    return 0
}

function Wait-And-Measure-Drain() {
    $drainTime = 0
    while ($drainTime -le 60) {
        $currentLag = Get-KafkaLag
        $currentDbLag = Get-DBLag
        if ([double]$currentLag -le 0 -and [double]$currentDbLag -le 0) {
            if ($drainTime -eq 0) { return "0s" }
            return "${drainTime}s"
        }
        Start-Sleep -Seconds 2
        $drainTime += 2
    }
    return "60s+ timeout"
}

$rates = @(50, 100, 200)
$duration = "60s"

foreach ($rps in $rates) {
    Write-Host "`n▶ Running $rps RPS Scenario (Duration: $duration)..."
    $summaryFile = "docs/performance/summary_${rps}.json"
    
    k6 run -e RPS=$rps -e DURATION=$duration --summary-export $summaryFile docs/performance/k6_load_test.js
    
    $summary = Get-Content -Path $summaryFile | ConvertFrom-Json
    
    $reqCount = 0; $actualTps = 0; $successRate = 0; $avgRes = 0; $p95Res = 0
    
    if ($summary.metrics.http_reqs) { 
        $reqCount = $summary.metrics.http_reqs.values.count 
        $actualTps = [math]::Round($summary.metrics.http_reqs.values.rate, 1)
    }
    if ($summary.metrics.alarm_success_rate) { $successRate = [math]::Round($summary.metrics.alarm_success_rate.values.rate * 100, 2) }
    if ($summary.metrics.http_req_duration) { 
        $avgRes = [math]::Round($summary.metrics.http_req_duration.values.avg)
        $p95Res = [math]::Round($summary.metrics.http_req_duration.values."p(95)")
    }

    $lagVal = Get-KafkaLag
    $lagInt = [math]::Round([double]$lagVal)
    $dbLagInt = Get-DBLag
    
    $queueState = "Kafka:$lagInt / DB:$dbLagInt"

    Write-Host "   Waiting for Queue Drain..."
    $drainTimeStr = Wait-And-Measure-Drain

    $actualTpsStr = "{0:0.0}" -f $actualTps
    $successRateStr = "{0:0.00}%" -f $successRate

    $row = "| **${rps} RPS** | $rps | $actualTpsStr | $reqCount | $successRateStr | ${avgRes}ms | ${p95Res}ms | ``$queueState`` | $drainTimeStr |"
    Add-Content -Path $mdFile -Value $row -Encoding UTF8
    
    Write-Host "   Finished $rps RPS -> Drain: $drainTimeStr"
    Remove-Item -Path $summaryFile -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 5
}
Write-Host "`n✅ All done! Results saved to $mdFile"