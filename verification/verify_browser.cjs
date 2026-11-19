const assert = require('node:assert/strict');
const net = require('node:net');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { chromium } = require(process.env.MOVIELENS_PLAYWRIGHT || 'playwright-core');
const fs = require('node:fs');

const stage = path.resolve(__dirname, '..', 'dashboard');
const images = path.join(__dirname, 'screenshots');
const logs = path.join(__dirname, 'logs');
const baseline = 'http://127.0.0.1:5066';
const faultTest = 'http://127.0.0.1:5067';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate, message, limit = 25000) {
  const started = Date.now();
  while (Date.now() - started < limit) {
    try { if (await predicate()) return; } catch {}
    await sleep(180);
  }
  throw new Error(message);
}

(async () => {
  fs.mkdirSync(images, { recursive: true });
  fs.mkdirSync(logs, { recursive: true });
  let browser, relay, flask;
  const sockets = new Set();
  const issues = [];
  const [kafkaHost, kafkaPort] = (process.env.MOVIELENS_KAFKA_BOOTSTRAP || 'namenode:9092').split(':');
  try {
    browser = await chromium.launch({ channel: 'msedge', headless: true });
    const page = await browser.newPage({ viewport: { width: 1440, height: 970 } });
    page.on('pageerror', error => issues.push(error.message));
    const ticks = [];
    page.on('request', request => { if (request.url().endsWith('/api/realtime')) ticks.push(Date.now()); });
    await page.goto(baseline, { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => document.querySelectorAll('.chart canvas').length === 10 && document.querySelectorAll('#recommendation-rows tr').length === 10, { timeout: 30000 });
    const calendar = () => page.locator('#heatmap-chart').evaluate(el => echarts.getInstanceByDom(el).getOption().series[0].data);
    const daily = (await (await fetch(baseline + '/api/offline/trend')).json()).daily;
    const dailyByDate = new Map(daily);
    const checkCalendar = async () => {
      assert.ok((await calendar()).every(([date, count]) => count === (dailyByDate.get(date) ?? 0)));
    };
    const originalYear = await page.locator('#calendar-year').inputValue();
    await checkCalendar();
    await page.locator('#calendar-year').selectOption('2000');
    assert.equal((await calendar()).length, 366);
    await page.locator('#calendar-period').selectOption('4');
    assert.equal((await calendar()).length, 92);
    await checkCalendar();
    await page.locator('#calendar-year').selectOption(originalYear);
    await page.locator('#calendar-period').selectOption('0');
    assert.equal(await page.locator('#total-ratings').innerText(), '1,000,209');
    assert.equal(await page.locator('#window-count').innerText(), '6');
    assert.ok(await page.locator('#alerts .alert-tone').count() > 0);
    assert.match(await page.locator('#recommendation-rows').innerText(), /Foreign Student/);
    await page.screenshot({ path: path.join(images, '09-echarts.png'), fullPage: true, animations: 'disabled' });
    await page.getByRole('button', {name:'类型', exact:true}).click();
    assert.equal(await page.locator('#breakdown-title').innerText(), '类型热度');
    await page.locator('#user-id').fill('2');
    await page.getByRole('button', {name:'查询推荐'}).click();
    await page.waitForFunction(() => document.querySelector('#recommend-status').textContent.includes('用户 2'));
    assert.equal(await page.locator('#recommendation-rows tr').count(), 10);
    await page.locator('#user-id').fill('999999');
    await page.getByRole('button', {name:'查询推荐'}).click();
    await page.waitForFunction(() => document.querySelector('#recommend-status').textContent.includes('没有推荐结果'));
    await until(() => ticks.length >= 3, 'Realtime polling was not observed');
    const intervals = ticks.slice(1, 3).map((time, i) => time - ticks[i]);
    assert.ok(intervals.every(ms => Math.abs(ms - 5000) < 1200), intervals);
    const viewportChecks = [];
    for (const width of [1440, 1024, 768, 390]) {
      await page.setViewportSize({width, height: 900});
      await sleep(120);
      const result = await page.evaluate(() => ({ width: innerWidth, scroll: document.documentElement.scrollWidth,
        charts: [...document.querySelectorAll('.chart canvas')].map(canvas => Math.round(canvas.getBoundingClientRect().width)) }));
      assert.equal(result.width, result.scroll);
      assert.ok(result.charts.every(chartWidth => chartWidth > 170 && chartWidth < width));
      viewportChecks.push(result.width);
    }
    await page.close();

    relay = net.createServer(client => {
      const kafka = net.createConnection({host: kafkaHost, port: Number(kafkaPort)});
      sockets.add(client); sockets.add(kafka);
      client.on('close', () => { sockets.delete(client); kafka.destroy(); });
      kafka.on('close', () => { sockets.delete(kafka); client.destroy(); });
      client.on('error', () => kafka.destroy()); kafka.on('error', () => client.destroy());
      client.pipe(kafka).pipe(client);
    });
    await new Promise((resolve, reject) => { relay.once('error', reject); relay.listen(0, '127.0.0.1', resolve); });
    flask = spawn(process.env.MOVIELENS_PYTHON || 'python', ['-B','app.py','--port','5067'], {
      cwd: stage, windowsHide: true, stdio: 'ignore',
      env: { ...process.env, MOVIELENS_KAFKA_BOOTSTRAP: `127.0.0.1:${relay.address().port}` },
    });
    await until(async () => (await fetch(faultTest)).ok, 'Fault-test Flask did not start');
    const outage = await browser.newPage({viewport:{width: 1440,height: 970}});
    outage.on('pageerror', error => issues.push(error.message));
    await outage.goto(faultTest, {waitUntil:'domcontentloaded'});
    await outage.waitForFunction(() => document.querySelector('#connection').dataset.state === 'online' && document.querySelector('#realtime-chart canvas'));
    const healthy = await (await fetch(faultTest+'/api/realtime')).json();
    assert.equal(healthy.kafka_reachable, true);
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => relay.close(resolve));
    relay = null;
    await outage.waitForFunction(() => document.querySelector('#connection-text').textContent === 'Kafka 不可达');
    const disconnected = await (await fetch(faultTest+'/api/realtime')).json();
    assert.equal(disconnected.kafka_reachable, false);
    assert.equal(disconnected.stale, true);
    assert.deepEqual(disconnected.windows, healthy.windows);
    assert.equal(await outage.locator('#window-count').innerText(), '6');
    assert.equal(await outage.locator('#realtime-chart canvas').count(), 1);
    assert.equal(await outage.locator('#average-chart canvas').count(), 1);
    assert.equal(await outage.locator('#alert-mix-chart canvas').count(), 1);
    assert.ok(await outage.locator('#alerts .alert-tone').count() > 0);
    await outage.screenshot({path: path.join(images, '09-echarts-kafka-cut.png'), fullPage: true, animations: 'disabled'});
    await outage.reload({waitUntil:'domcontentloaded'});
    await outage.waitForFunction(() => document.querySelector('#connection-text').textContent === 'Kafka 不可达' && document.querySelector('#realtime-chart canvas') && document.querySelectorAll('#recommendation-rows tr').length === 10);
    assert.equal(await outage.locator('#window-count').innerText(), '6');
    assert.deepEqual(issues, []);
    const report = { browser:'Edge', charts:10, calendarYearAndQuarterChecked:true, viewportChecks, realtimePollIntervalsMs:intervals, userRecommendations:10, brokerDisconnectRetainedWindows:disconnected.windows.length, pageErrors:issues.length };
    fs.writeFileSync(path.join(logs, '09-echarts.json'), JSON.stringify(report, null, 2) + '\n');
    console.log(JSON.stringify(report));
  } finally {
    for (const socket of sockets) socket.destroy();
    if (relay) relay.close();
    if (flask) flask.kill();
    if (browser) await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode=1; });
