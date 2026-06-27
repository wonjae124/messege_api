param(
    [string]$outputDir = "docs/performance"
)

Write-Host "=======================================" -ForegroundColor Cyan
Write-Host "  Load Test A/B/C (Tuned) - Automated" -ForegroundColor Cyan
Write-Host "=======================================" -ForegroundColor Cyan

# --- Helper Functions ---
function Get-OutboxPendingCount {
    try {
        $output = (docker exec postgres-perf psql -U alarmuser -d alarmdb -t -c "SELECT count(*) FROM alarm_outbox_events WHERE status = 'PENDING';") -join ''
        $trimmed = $output.Trim()
        if (-not [string]::IsNullOrWhiteSpace($trimmed)) {
            return [int]$trimmed
        }
    } catch {
        Write-Host "     [WARN] Failed to get outbox count: $_" -ForegroundColor DarkYellow
    }
    return -1
}

function Reset-Environment {
    Write-Host "   Resetting environment..." -ForegroundColor DarkGray
    # Wait for pending to drain
    $waitCount = 0
    while ($waitCount -lt 15) {
        $pending = Get-OutboxPendingCount
        if ($pending -le 0) { break }
        Write-Host "     Waiting for drain (PENDING: $pending)..." -ForegroundColor DarkGray
        Start-Sleep -Seconds 2
        $waitCount++
    }
    # Clean outbox
    docker exec postgres-perf psql -U alarmuser -d alarmdb -c "DELETE FROM alarm_outbox_events;" 2>$null | Out-Null
    Start-Sleep -Seconds 2
}

function Get-CBBlockedCount {
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/query?query=sum(resilience4j_circuitbreaker_calls_total{kind=%27not_permitted%27})" -TimeoutSec 5
        if ($response.data.result.Length -gt 0) {
            return [double]$response.data.result[0].value[1]
        }
    } catch {
        Write-Host "     [WARN] Failed to get CB count: $_" -ForegroundColor DarkYellow
    }
    return 0
}

function Measure-PendingDrain {
    $startTime = Get-Date
    $maxWait = 30
    while ($true) {
        $elapsed = [math]::Round(((Get-Date) - $startTime).TotalSeconds, 1)
        if ($elapsed -ge $maxWait) { return -1 }
        $pending = Get-OutboxPendingCount
        if ($pending -eq 0) {
            return $elapsed
        }
        if ($pending -gt 0) {
            Write-Host "     ... Draining (PENDING: $pending, Elapsed: ${elapsed}s)" -ForegroundColor DarkGray
        }
        Start-Sleep -Milliseconds 500
    }
}

function Run-Scenario {
    param(
        [string]$Name,
        [int]$RPS,
        [string]$Duration = "20s"
    )

    Write-Host "`n========================================" -ForegroundColor Yellow
    Write-Host "  Scenario: $Name ($RPS RPS / $Duration)" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Yellow

    # Reset environment
    Reset-Environment

    # Record CB blocked count before test
    $cbBefore = Get-CBBlockedCount
    Write-Host "   CB Blocked Count (before): $cbBefore" -ForegroundColor DarkGray

    # Run K6 test
    $summaryFile = "$outputDir/summary_${Name}.json"
    Write-Host "   Running K6 ($RPS RPS for $Duration)..." -ForegroundColor Green

    k6 run -e RPS=$RPS -e DURATION=$Duration --summary-export $summaryFile docs/performance/k6_load_test.js 2>&1 | ForEach-Object { Write-Host "   [K6] $_" -ForegroundColor DarkGray }

    Write-Host "   K6 finished. Measuring PENDING drain..." -ForegroundColor Green

    # Measure drain time
    $drainTime = Measure-PendingDrain

    # Record CB blocked count after test
    $cbAfter = Get-CBBlockedCount
    $cbDelta = [math]::Round($cbAfter - $cbBefore)

    # Parse K6 summary JSON
    $reqCount = 0; $actualTps = 0; $avgRes = 0; $p95Res = 0; $failCount = 0
    if (Test-Path $summaryFile) {
        try {
            $raw = Get-Content -Path $summaryFile -Raw
            $summary = $raw | ConvertFrom-Json

            # http_reqs
            if ($summary.metrics.http_reqs.values) {
                $reqCount = $summary.metrics.http_reqs.values.count
                $actualTps = [math]::Round($summary.metrics.http_reqs.values.rate, 1)
            } elseif ($summary.metrics.http_reqs.count) {
                $reqCount = $summary.metrics.http_reqs.count
                $actualTps = [math]::Round($summary.metrics.http_reqs.rate, 1)
            }

            # http_req_duration
            if ($summary.metrics.http_req_duration.values) {
                $avgRes = [math]::Round($summary.metrics.http_req_duration.values.avg, 2)
                $p95Val = $summary.metrics.http_req_duration.values | Select-Object -ExpandProperty 'p(95)' -ErrorAction SilentlyContinue
                if ($p95Val) { $p95Res = [math]::Round($p95Val, 2) }
            } elseif ($summary.metrics.http_req_duration.avg) {
                $avgRes = [math]::Round($summary.metrics.http_req_duration.avg, 2)
            }

            # http_req_failed
            if ($summary.metrics.http_req_failed.values) {
                $failRate = $summary.metrics.http_req_failed.values.rate
                if ($failRate -and $reqCount) {
                    $failCount = [math]::Round($reqCount * $failRate)
                }
            }
        } catch {
            Write-Host "   [WARN] Failed to parse summary: $_" -ForegroundColor DarkYellow
        }
    } else {
        Write-Host "   [WARN] Summary file not found: $summaryFile" -ForegroundColor DarkYellow
    }

    Write-Host "`n   --- Results for $Name ---" -ForegroundColor Cyan
    Write-Host "   Total Requests: $reqCount"
    Write-Host "   Actual TPS: $actualTps"
    Write-Host "   Avg Response: ${avgRes}ms"
    Write-Host "   p95 Response: ${p95Res}ms"
    Write-Host "   Failed: $failCount"
    Write-Host "   CB Blocked (delta): $cbDelta"
    Write-Host "   Drain Time: ${drainTime}s"

    # Build result as ordered hashtable for JSON serialization
    $result = [ordered]@{
        Name      = $Name
        RPS       = $RPS
        Duration  = $Duration
        ReqCount  = $reqCount
        ActualTPS = $actualTps
        AvgRes    = $avgRes
        P95Res    = $p95Res
        FailCount = $failCount
        CBBlocked = $cbDelta
        DrainTime = $drainTime
    }

    # Cleanup summary file
    Remove-Item -Path $summaryFile -ErrorAction SilentlyContinue

    return $result
}

# --- Main Execution ---

# Warm-up
Write-Host "`n Warm-up: 10 RPS for 10s..." -ForegroundColor Magenta
k6 run -e RPS=10 -e DURATION=10s docs/performance/k6_load_test.js 2>&1 | Out-Null
Start-Sleep -Seconds 5
Reset-Environment
Start-Sleep -Seconds 3

# Run scenarios A, B, C
$resultA = Run-Scenario -Name "A-Tuned" -RPS 100 -Duration "20s"
Start-Sleep -Seconds 10

$resultB = Run-Scenario -Name "B-Tuned" -RPS 300 -Duration "20s"
Start-Sleep -Seconds 10

$resultC = Run-Scenario -Name "C-Tuned" -RPS 500 -Duration "20s"

# --- Output Results as JSON ---
$allResults = @($resultA, $resultB, $resultC)
$jsonOutput = $allResults | ConvertTo-Json -Depth 5
$jsonFile = "$outputDir/abc_tuned_results.json"
Set-Content -Path $jsonFile -Value $jsonOutput -Encoding UTF8

Write-Host "`n=======================================" -ForegroundColor Green
Write-Host "  All scenarios complete!" -ForegroundColor Green
Write-Host "  Results saved to: $jsonFile" -ForegroundColor Green
Write-Host "=======================================" -ForegroundColor Green

# Print summary table
Write-Host "`n  === Summary Table ===" -ForegroundColor Cyan
Write-Host ("  {0,-15} {1,-10} {2,-10} {3,-10} {4,-12} {5,-15} {6,-10}" -f "Scenario", "Requests", "TPS", "AvgMs", "p95Ms", "Failed", "CB Blocked")
foreach ($r in $allResults) {
    Write-Host ("  {0,-15} {1,-10} {2,-10} {3,-10} {4,-12} {5,-15} {6,-10}" -f $r.Name, $r.ReqCount, $r.ActualTPS, $r.AvgRes, $r.P95Res, $r.FailCount, $r.CBBlocked)
}
