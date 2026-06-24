const { execSync } = require('child_process');
const fs = require('fs');

const mdFile = 'docs/performance/PERFORMANCE_RPS_TABLE.md';

console.log('=======================================');
console.log('🚀 Starting Automated RPS Table Extraction...');
console.log('=======================================');

const now = new Date().toLocaleString('ko-KR');
const header = `\n## 🧪 부하 테스트 실행 기록: ${now}\n\n**테스트 요약**: (테스트 결과에 대한 코멘트를 작성할 수 있습니다)\n\n| 시나리오 | Target RPS | Actual TPS | 총 요청 수 | API 성공률 | 평균 응답시간 | p95 응답시간 | 테스트 직후 큐 상태(Kafka/DB) | Drain |\n| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n`;

if (!fs.existsSync(mdFile)) {
    fs.writeFileSync(mdFile, `# 📈 성능 테스트 히스토리 (RPS)\n`, 'utf8');
}
fs.appendFileSync(mdFile, header, 'utf8');

function getKafkaLag() {
    const cmd = `docker exec kafka-perf kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group alarm_api_consumer_group 2>NUL`;
    try {
        const output = execSync(cmd, { encoding: 'utf8', timeout: 5000 });
        const lines = output.split('\n');
        let totalLag = 0;
        let hasData = false;
        for (const line of lines) {
            const parts = line.trim().split(/\s+/);
            if (parts.length >= 6 && !isNaN(parseInt(parts[5]))) {
                totalLag += parseInt(parts[5]);
                hasData = true;
            }
        }
        return hasData ? totalLag : -1;
    } catch (e) {
        return -1;
    }
}

function getOutboxCount() {
    const cmd = `docker exec postgres-perf psql -U alarmuser -d alarmdb -t -c "SELECT count(*) FROM alarm_outbox_events WHERE status = 'PENDING';" 2>NUL`;
    try {
        const output = execSync(cmd, { encoding: 'utf8', timeout: 5000 });
        const val = parseInt(output.trim());
        return isNaN(val) ? -1 : val;
    } catch (e) {
        return -1;
    }
}

(async () => {
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    async function measureDrain() {
        let drainTime = 0;
        while (drainTime <= 60) {
            const lag = getKafkaLag();
            const outbox = getOutboxCount();
            if (lag === 0 && outbox === 0) {
                return drainTime === 0 ? "0s" : `${drainTime}s`;
            }
            const displayLag = lag === -1 ? "TIMEOUT" : lag;
            const displayOutbox = outbox === -1 ? "TIMEOUT" : outbox;
            console.log(`     ... Still draining (Kafka Lag: ${displayLag}, DB Outbox: ${displayOutbox}, Elapsed: ${drainTime}s)`);
            await sleep(2000); 
            drainTime += 2;
        }
        return "60s+ 미해소";
    }

    console.log(`\n▶ Running Warm-up (10 RPS for 60s)...`);
    try {
        execSync(`k6 run -e RPS=10 -e DURATION=60s docs/performance/k6_load_test.js`, { stdio: 'ignore' });
    } catch(e) {}
    console.log(`   Waiting for Queue Drain after Warm-up...`);
    await measureDrain();
    await sleep(5000);

    const rates = [50, 100, 200];

    for (const rps of rates) {
        const duration = "60s";
        console.log(`\n▶ Running ${rps} RPS Scenario (Duration: ${duration})...`);
        
        const summaryFile = `docs/performance/summary_${rps}.json`;
        try {
            execSync(`k6 run -e RPS=${rps} -e DURATION=${duration} --summary-export ${summaryFile} docs/performance/k6_load_test.js`, { stdio: 'inherit' });
        } catch(e) {}
        
        let reqCount = 0, successRate = 0, avgRes = 0, p95Res = 0, actualTps = 0;
        if (fs.existsSync(summaryFile)) {
            const summary = JSON.parse(fs.readFileSync(summaryFile, 'utf8'));
            if (summary.metrics.http_reqs) {
                reqCount = summary.metrics.http_reqs.count || (summary.metrics.http_reqs.values && summary.metrics.http_reqs.values.count) || 0;
                actualTps = summary.metrics.http_reqs.rate || (summary.metrics.http_reqs.values && summary.metrics.http_reqs.values.rate) || 0;
            }
            if (summary.metrics.alarm_success_rate) {
                const rate = summary.metrics.alarm_success_rate.value || summary.metrics.alarm_success_rate.rate || 0;
                successRate = (rate * 100).toFixed(2);
            }
            if (summary.metrics.http_req_duration) {
                const avg = summary.metrics.http_req_duration.avg || (summary.metrics.http_req_duration.values && summary.metrics.http_req_duration.values.avg) || 0;
                const p95 = summary.metrics.http_req_duration['p(95)'] || (summary.metrics.http_req_duration.values && summary.metrics.http_req_duration.values['p(95)']) || 0;
                avgRes = Math.round(avg);
                p95Res = Math.round(p95);
            }
            fs.unlinkSync(summaryFile);
        }
        
        const lagVal = getKafkaLag();
        const outboxVal = getOutboxCount();
        const queueState = `Kafka:${lagVal === -1 ? 'Err' : lagVal} / DB:${outboxVal === -1 ? 'Err' : outboxVal}`;
        
        console.log(`   Waiting for Queue Drain...`);
        const drainTimeStr = await measureDrain();
        
        const row = `| **${rps} RPS** | ${rps} | ${actualTps.toFixed(1)} | ${reqCount} | ${successRate}% | ${avgRes}ms | ${p95Res}ms | \`${queueState}\` | ${drainTimeStr} |\n`;
        fs.appendFileSync(mdFile, row, 'utf8');
        
        console.log(`   Finished ${rps} RPS -> Drain: ${drainTimeStr}`);
        await sleep(5000); // Reset queue
    }

    console.log(`\n✅ All done! Results saved to ${mdFile}`);
})();
