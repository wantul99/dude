// ============================================================
//  MIKROTIK PING MONITOR — Server dengan Log History
// ============================================================
const express = require('express');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { RouterOSAPI } = require('node-routeros');

const app = express();
const PORT = parseInt(process.env.PORT, 10) || 3000;

// ============================================================
//  ⚙️  KONFIGURASI
// ============================================================
const MIKROTIK_CONFIG = {
    host:     process.env.MK_HOST || '192.168.90.11',
    user:     process.env.MK_USER || 'w2n',
    password: process.env.MK_PASS || 'w2n',
    port:     parseInt(process.env.MK_PORT, 10) || 8728,
    timeout:  parseInt(process.env.MK_TIMEOUT, 10) || 5
};

const CONFIG = {
    PING_INTERVAL_MS:    parseInt(process.env.PING_INTERVAL_MS, 10) || 2000,
    PING_BATCH_SIZE:     parseInt(process.env.PING_BATCH_SIZE, 10) || 64,
    PING_COUNT:          parseInt(process.env.PING_COUNT, 10) || 1,
    SIAGA_THRESHOLD_MS:  parseInt(process.env.SIAGA_MS, 10) || 120,
    HISTORY_SIZE:        parseInt(process.env.HISTORY_SIZE, 10) || 10,
    UNSTABLE_FAIL_RATE:  parseFloat(process.env.UNSTABLE_FAIL_RATE) || 0.5,
    OFFLINE_AFTER_MS:    parseInt(process.env.OFFLINE_AFTER_MS, 10) || 60000,
    POOL_SIZE:           parseInt(process.env.POOL_SIZE, 10) || 8,
    OFFLINE_SKIP_EVERY:  parseInt(process.env.OFFLINE_SKIP_EVERY, 10) || 3,
    SAVE_DEBOUNCE_MS:    parseInt(process.env.SAVE_DEBOUNCE_MS, 10) || 5000,
    SCAN_CONCURRENCY:    parseInt(process.env.SCAN_CONCURRENCY, 10) || 32,
    FAST_RECOVERY_N:     parseInt(process.env.FAST_RECOVERY_N, 10) || 3,
    FAST_OFFLINE_N:      parseInt(process.env.FAST_OFFLINE_N, 10) || 5,

    // 📋 LOG
    MAX_LOGS:            parseInt(process.env.MAX_LOGS, 10) || 20000,
    LOG_SAVE_DEBOUNCE_MS: parseInt(process.env.LOG_SAVE_DEBOUNCE_MS, 10) || 3000,

    VERBOSE: process.env.VERBOSE === '1',
    DEBUG_PING: process.env.DEBUG_PING === '1'
};

const PING_INTERVAL_MS    = CONFIG.PING_INTERVAL_MS;
const PING_BATCH_SIZE     = CONFIG.PING_BATCH_SIZE;
const SIAGA_THRESHOLD_MS  = CONFIG.SIAGA_THRESHOLD_MS;
const PING_COUNT          = CONFIG.PING_COUNT;
const OFFLINE_AFTER_MS    = CONFIG.OFFLINE_AFTER_MS;
const HISTORY_SIZE        = CONFIG.HISTORY_SIZE;
const UNSTABLE_FAIL_RATE  = CONFIG.UNSTABLE_FAIL_RATE;

// ============================================================
//  🌐  STATE KONEKSI
// ============================================================
let apiLatencyMs = -1;
let apiStatus = 'unknown';
let lastApiOkAt = 0;
let apiFailCount = 0;

// ============================================================
//  📁  DATABASE JSON
// ============================================================
const DATA_DIR  = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'devices.json');
const LOG_FILE  = path.join(DATA_DIR, 'logs.json');

if (!fs.existsSync(DATA_DIR))  fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(DATA_FILE)) fs.writeFileSync(DATA_FILE, '[]', 'utf8');
if (!fs.existsSync(LOG_FILE))  fs.writeFileSync(LOG_FILE, '[]', 'utf8');

let DEVICES = [];
let LOGS = [];
let saveTimer = null;
let logSaveTimer = null;
let logDirty = false;
let cycleCounter = 0;

function loadDevicesFromFile() {
    try {
        const raw = fs.readFileSync(DATA_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        const now = Date.now();
        DEVICES = Array.isArray(parsed) ? parsed.map(d => ({
            ip: String(d.ip || '').trim(),
            deviceName: String(d.deviceName || '').trim(),
            status: d.status || 'Memeriksa...',
            responseTime: d.responseTime || 'N/A',
            lastChecked: d.lastChecked || '-',
            lastOnlineAt: (typeof d.lastOnlineAt === 'number' && d.lastOnlineAt > 0)
                ? d.lastOnlineAt
                : (typeof d.statusSince === 'number' && d.statusSince > 0 ? d.statusSince : now),
            lastSuccessMs: d.lastSuccessMs || 0,
            consecutiveFailures: d.consecutiveFailures || 0,
            consecutiveSuccess: d.consecutiveSuccess || 0,
            statusSince: (typeof d.statusSince === 'number' && d.statusSince > 0) ? d.statusSince : now,
            recentResults: Array.isArray(d.recentResults)
                ? d.recentResults.slice(-HISTORY_SIZE).map(v => v ? 1 : 0)
                : []
        })).filter(d => d.ip) : [];
        console.log(`📄 Dimuat ${DEVICES.length} device dari file`);
    } catch (e) {
        console.error('❌ Gagal baca devices.json:', e.message);
        DEVICES = [];
    }
}

function loadLogsFromFile() {
    try {
        const raw = fs.readFileSync(LOG_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        LOGS = Array.isArray(parsed) ? parsed.filter(l => l && l.timestamp && l.ip) : [];
        console.log(`📋 Dimuat ${LOGS.length} log dari file`);
    } catch (e) {
        console.error('❌ Gagal baca logs.json:', e.message);
        LOGS = [];
    }
}

function saveNow() {
    try {
        const tmp = DATA_FILE + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(DEVICES, null, 2), 'utf8');
        fs.renameSync(tmp, DATA_FILE);
        return { ok: true };
    } catch (e) {
        return { ok: false, message: e.message };
    }
}

function saveLogsNow() {
    try {
        const tmp = LOG_FILE + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(LOGS), 'utf8');  // no indent — hemat disk
        fs.renameSync(tmp, LOG_FILE);
        logDirty = false;
        return { ok: true };
    } catch (e) {
        console.error('❌ Gagal simpan logs.json:', e.message);
        return { ok: false, message: e.message };
    }
}

function markDirty() {
    if (saveTimer) return;
    saveTimer = setTimeout(() => {
        saveTimer = null;
        const r = saveNow();
        if (r.ok && CONFIG.VERBOSE) console.log('💾 Auto-save devices.json');
    }, CONFIG.SAVE_DEBOUNCE_MS);
}

function markLogsDirty() {
    logDirty = true;
    if (logSaveTimer) return;
    logSaveTimer = setTimeout(() => {
        logSaveTimer = null;
        if (logDirty) saveLogsNow();
    }, CONFIG.LOG_SAVE_DEBOUNCE_MS);
}

// ═══════════════════════════════════════════════════════════
//  📋 LOG — catat transisi status
// ═══════════════════════════════════════════════════════════
function getLogType(fromStatus, toStatus) {
    const isBad = (s) => s === 'Unstable' || s === 'Offline';
    const isGood = (s) => s === 'Online' || s === 'Siaga';

    if (isBad(toStatus) && !isBad(fromStatus)) return 'problem';
    if (isGood(toStatus) && isBad(fromStatus)) return 'recovery';
    if (toStatus === 'Siaga' && fromStatus === 'Online') return 'warning';
    if (toStatus === 'Online' && fromStatus === 'Siaga') return 'warning';  // Siaga → Online juga informatif
    if (isBad(toStatus) && isBad(fromStatus)) return 'problem';  // Unstable → Offline
    return 'other';
}

function recordLog(device, fromStatus, toStatus) {
    if (!fromStatus || fromStatus === 'Memeriksa...' || !toStatus) return;
    if (fromStatus === toStatus) return;

    const entry = {
        id: crypto.randomBytes(6).toString('hex'),
        timestamp: Date.now(),
        ip: device.ip,
        deviceName: device.deviceName || '',
        fromStatus,
        toStatus,
        type: getLogType(fromStatus, toStatus)
    };

    LOGS.push(entry);

    // Trim jika melebihi max
    if (LOGS.length > CONFIG.MAX_LOGS) {
        const drop = LOGS.length - CONFIG.MAX_LOGS;
        LOGS.splice(0, drop);
        console.log(`📋 Log trimmed: ${drop} entri lama dihapus (max ${CONFIG.MAX_LOGS})`);
    }

    markLogsDirty();

    if (CONFIG.VERBOSE) {
        console.log(`📋 LOG: ${device.ip} ${fromStatus} → ${toStatus} [${entry.type}]`);
    }
}

// ============================================================
//  🔌  CONNECTION POOL
// ============================================================
class ConnPool {
    constructor(size, config) {
        this.size = size;
        this.config = config;
        this.conns = new Array(size).fill(null);
        this.connecting = new Array(size).fill(null);
        this.rr = 0;
        this.connectedCount = 0;
    }
    async get() {
        const idx = this.rr;
        this.rr = (this.rr + 1) % this.size;
        const conn = await this.getSlot(idx);
        return { conn, slot: idx };
    }
    async getSlot(idx) {
        if (this.conns[idx]) return this.conns[idx];
        if (this.connecting[idx]) return this.connecting[idx];
        this.connecting[idx] = (async () => {
            const c = new RouterOSAPI({ ...this.config });
            await c.connect();
            this.conns[idx] = c;
            this.connectedCount++;
            if (CONFIG.VERBOSE) console.log(`🔌 Pool slot #${idx} connected`);
            return c;
        })();
        try { return await this.connecting[idx]; }
        finally { this.connecting[idx] = null; }
    }
    resetSlot(idx) {
        if (this.conns[idx]) {
            try { this.conns[idx].close(); } catch (e) {}
            this.conns[idx] = null;
            this.connectedCount = Math.max(0, this.connectedCount - 1);
        }
    }
    async closeAll() {
        for (let i = 0; i < this.size; i++) this.resetSlot(i);
    }
}

const pool = new ConnPool(CONFIG.POOL_SIZE, MIKROTIK_CONFIG);

// ============================================================
//  📡  PING
// ============================================================
function parsePingResult(pingResult, expectedCount) {
    let received = 0, totalTime = 0, rawReplyCount = 0;
    let packetLoss = null, sent = null;
    const list = Array.isArray(pingResult) ? pingResult : (pingResult ? [pingResult] : []);
    for (const r of list) {
        if (!r || typeof r !== 'object') continue;
        if (r.received !== undefined && r.received !== '') {
            const n = parseInt(r.received, 10);
            if (!isNaN(n)) received = n;
        }
        if (r.sent !== undefined && r.sent !== '') {
            const n = parseInt(r.sent, 10);
            if (!isNaN(n)) sent = n;
        }
        if (r['packet-loss'] !== undefined && r['packet-loss'] !== '') {
            const n = parseFloat(r['packet-loss']);
            if (!isNaN(n)) packetLoss = n;
        }
        if (r.time !== undefined && r.time !== '' && r.time !== null) {
            rawReplyCount++;
            const t = String(r.time).toLowerCase();
            const num = parseFloat(t) || 0;
            if (t.includes('us')) totalTime += num / 1000;
            else totalTime += num;
        }
    }
    if (received === 0 && rawReplyCount > 0) received = rawReplyCount;
    if (received === 0 && packetLoss !== null && packetLoss < 100) {
        const base = sent || expectedCount || 1;
        received = Math.round((1 - packetLoss / 100) * base);
    }
    const online = received > 0;
    const avgTime = received > 0 ? Math.max(1, Math.round(totalTime / received)) + ' ms' : 'timeout';
    return { online, time: avgTime, received, rawReplyCount, packetLoss };
}

async function pingOnce(ip, count = PING_COUNT) {
    const startTime = Date.now();
    let slot = -1;
    try {
        const g = await pool.get();
        slot = g.slot;
        const conn = g.conn;
        const pingResult = await conn.write('/ping', [
            '=address=' + ip, '=count=' + count, '=size=56'
        ]);
        apiLatencyMs = Date.now() - startTime;
        apiStatus = 'live';
        lastApiOkAt = Date.now();
        apiFailCount = 0;
        const parsed = parsePingResult(pingResult, count);
        if (CONFIG.DEBUG_PING || CONFIG.VERBOSE) {
            console.log(`📡 ping ${ip} → online=${parsed.online} recv=${parsed.received} raw=${parsed.rawReplyCount} time=${parsed.time}`);
        }
        return { online: parsed.online, time: parsed.time };
    } catch (err) {
        if (slot >= 0) pool.resetSlot(slot);
        apiFailCount++;
        if (apiFailCount >= 3) { apiStatus = 'down'; apiLatencyMs = -1; }
        if (CONFIG.VERBOSE) console.error('⚠️ Ping error [' + ip + ']:', err.message);
        return { online: false, time: 'Err' };
    }
}

function parseMs(timeStr) {
    if (!timeStr) return -1;
    const s = String(timeStr).toLowerCase();
    if (s.includes('timeout') || s.includes('err')) return -1;
    const m = s.match(/(\d+(?:\.\d+)?)/);
    return m ? parseFloat(m[1]) : -1;
}

function getCategory(status) {
    if (status === 'Online' || status === 'Siaga') return 'up';
    if (status === 'Unstable' || status === 'Offline') return 'down';
    return 'unknown';
}

function getDurationBase(device, now) {
    const cat = getCategory(device.status);
    if (cat === 'up') return device.statusSince || now;
    if (typeof device.lastOnlineAt === 'number' && device.lastOnlineAt > 0) return device.lastOnlineAt;
    return device.statusSince || now;
}

function getDurationType(device) {
    const cat = getCategory(device.status);
    if (cat === 'up') return 'uptime';
    if (cat === 'down') return 'downtime';
    return 'unknown';
}

function determineStatus(online, responseTime, device) {
    const now = Date.now();
    const ms = online ? parseMs(responseTime) : -1;

    if (!Array.isArray(device.recentResults)) device.recentResults = [];
    device.recentResults.push(online ? 1 : 0);
    while (device.recentResults.length > HISTORY_SIZE) device.recentResults.shift();

    if (online) {
        device.lastOnlineAt = now;
        device.consecutiveFailures = 0;
        device.consecutiveSuccess = (device.consecutiveSuccess || 0) + 1;
        if (ms >= 0) device.lastSuccessMs = ms;
    } else {
        device.consecutiveFailures = (device.consecutiveFailures || 0) + 1;
        device.consecutiveSuccess = 0;
    }

    const window = device.recentResults;
    const total = window.length;
    const successCount = window.reduce((a, b) => a + b, 0);
    const failRate = total > 0 ? (total - successCount) / total : 0;

    const fastRecovery = device.consecutiveSuccess >= CONFIG.FAST_RECOVERY_N;
    const elapsedNoSuccess = now - (device.lastOnlineAt || 0);
    const fastOffline = device.consecutiveFailures >= CONFIG.FAST_OFFLINE_N && elapsedNoSuccess >= OFFLINE_AFTER_MS;

    let newStatus;
    if (total < 2) {
        if (!online) newStatus = 'Unstable';
        else newStatus = (ms > SIAGA_THRESHOLD_MS) ? 'Siaga' : 'Online';
    }
    else if (fastRecovery) {
        const refMs = ms >= 0 ? ms : (device.lastSuccessMs || 0);
        newStatus = (refMs > SIAGA_THRESHOLD_MS) ? 'Siaga' : 'Online';
    }
    else if (fastOffline) newStatus = 'Offline';
    else if (successCount === 0 && elapsedNoSuccess >= OFFLINE_AFTER_MS) newStatus = 'Offline';
    else if (failRate > UNSTABLE_FAIL_RATE) newStatus = 'Unstable';
    else {
        const refMs = ms >= 0 ? ms : (device.lastSuccessMs || 0);
        newStatus = (refMs > SIAGA_THRESHOLD_MS) ? 'Siaga' : 'Online';
    }

    // 📋 Log transisi (sebelum update status)
    const prevStatus = device.status;
    if (prevStatus !== newStatus && prevStatus !== 'Memeriksa...') {
        recordLog(device, prevStatus, newStatus);
    }

    const prevCat = getCategory(device.status);
    const newCat = getCategory(newStatus);
    if (prevCat !== newCat || !device.statusSince) {
        device.statusSince = now;
        if (prevCat !== 'unknown' && prevCat !== newCat) {
            console.log(`⏱️  ${device.ip}: ${device.status} → ${newStatus}`);
        }
    }

    return newStatus;
}

function shouldPingThisCycle(device) {
    if (device.status !== 'Offline') return true;
    return (cycleCounter % CONFIG.OFFLINE_SKIP_EVERY) === 0;
}

// ============================================================
//  🔄  BACKGROUND PING LOOP
// ============================================================
let loopRunning = false;

async function backgroundPingLoop() {
    if (loopRunning) return;
    loopRunning = true;

    console.log(`🔄 Background ping loop dimulai`);
    console.log(`   • Interval      : ${PING_INTERVAL_MS} ms`);
    console.log(`   • Pool size     : ${CONFIG.POOL_SIZE}`);
    console.log(`   • Siaga >       : ${SIAGA_THRESHOLD_MS} ms`);
    console.log(`   • Unstable jika : down > ${Math.round(UNSTABLE_FAIL_RATE*100)}%`);
    console.log(`   • Offline jika  : ${CONFIG.FAST_OFFLINE_N} gagal + ${OFFLINE_AFTER_MS/1000}s`);
    console.log(`   • Max logs      : ${CONFIG.MAX_LOGS} entri`);

    while (true) {
        cycleCounter++;
        const t0 = Date.now();
        const now = new Date().toLocaleTimeString('id-ID');

        try {
            if (DEVICES.length > 0) {
                const toPing = DEVICES.filter(shouldPingThisCycle);
                const skipped = DEVICES.length - toPing.length;

                for (let i = 0; i < toPing.length; i += PING_BATCH_SIZE) {
                    const batch = toPing.slice(i, i + PING_BATCH_SIZE);
                    await Promise.all(batch.map(async (d) => {
                        const result = await pingOnce(d.ip, PING_COUNT);
                        d.status = determineStatus(result.online, result.time, d);
                        d.responseTime = result.time;
                        d.lastChecked = now;
                    }));
                }

                markDirty();

                const elapsed = Date.now() - t0;
                const skipInfo = skipped > 0 ? ` (skip ${skipped} offline)` : '';
                console.log(`✓ Siklus #${cycleCounter}: ${toPing.length}/${DEVICES.length} device dalam ${elapsed}ms${skipInfo}`);
            } else {
                const startTime = Date.now();
                try {
                    const g = await pool.get();
                    await g.conn.write('/system/identity/print');
                    apiLatencyMs = Date.now() - startTime;
                    apiStatus = 'live';
                    lastApiOkAt = Date.now();
                    apiFailCount = 0;
                } catch (e) {
                    apiFailCount++;
                    if (apiFailCount >= 3) { apiStatus = 'down'; apiLatencyMs = -1; }
                }
            }
        } catch (e) {
            console.error('❌ Loop error:', e.message);
        }

        const elapsed = Date.now() - t0;
        const wait = Math.max(200, PING_INTERVAL_MS - elapsed);
        await new Promise(r => setTimeout(r, wait));
    }
}

// ============================================================
//  ⚡ SCAN SUBNET (polling-based)
// ============================================================
const SCANS = new Map();
function makeScanId() { return crypto.randomBytes(8).toString('hex'); }

async function runScan(scan) {
    const CONC = Math.min(CONFIG.SCAN_CONCURRENCY, scan.total);
    let cursor = scan.start;
    scan.startedAt = Date.now();
    scan.running = true;

    const workers = Array.from({ length: CONC }, async () => {
        while (true) {
            if (scan.aborted) return;
            const i = cursor++;
            if (i > scan.end) return;
            const ip = `${scan.prefix}.${i}`;
            try {
                const r = await pingOnce(ip, 1);
                if (r.online) {
                    scan.found++;
                    scan.online.push({ ip, time: r.time });
                }
            } catch (e) {}
            scan.scanned++;
        }
    });
    await Promise.all(workers);

    scan.done = true;
    scan.running = false;
    scan.finishedAt = Date.now();
    scan.online.sort((a, b) => ipToNum(a.ip) - ipToNum(b.ip));
    console.log(`⚡ Scan ${scan.prefix}.${scan.start}-${scan.end}: ${scan.found} online / ${scan.scanned} dalam ${scan.finishedAt - scan.startedAt}ms`);
    setTimeout(() => SCANS.delete(scan.scanId), 5 * 60 * 1000);
}

function ipToNum(ip) {
    return ip.split('.').reduce((acc, o) => (acc << 8) + parseInt(o, 10), 0) >>> 0;
}

function formatDuration(ms) {
    if (!ms || ms < 0 || !isFinite(ms)) return '—';
    const totalSec = Math.floor(ms / 1000);
    if (totalSec < 60) return `${totalSec}d`;
    const totalMin = Math.floor(totalSec / 60);
    if (totalMin < 60) {
        const s = totalSec % 60;
        return s > 0 ? `${totalMin}m ${s}d` : `${totalMin}m`;
    }
    const totalHr = Math.floor(totalMin / 60);
    if (totalHr < 24) {
        const m = totalMin % 60;
        return m > 0 ? `${totalHr}j ${m}m` : `${totalHr}j`;
    }
    const totalDay = Math.floor(totalHr / 24);
    const h = totalHr % 24;
    return h > 0 ? `${totalDay}h ${h}j` : `${totalDay}h`;
}

// ============================================================
//  MIDDLEWARE
// ============================================================
app.use(express.json({ limit: '5mb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ============================================================
//  📡  API: PING manual
// ============================================================
app.post('/api/ping', async (req, res) => {
    const { target, count } = req.body;
    if (!target) return res.status(400).json({ success: false, message: 'target wajib diisi' });
    const pingCount = Math.min(Math.max(parseInt(count) || 1, 1), 10);
    const result = await pingOnce(target, pingCount);
    res.json({ success: true, online: result.online, time: result.time });
});

// ============================================================
//  🔍 DEBUG
// ============================================================
app.get('/api/debug/ping/:ip', async (req, res) => {
    const ip = req.params.ip;
    if (!/^[0-9a-zA-Z\.\-]+$/.test(ip)) return res.status(400).json({ success: false, message: 'IP tidak valid' });
    try {
        const g = await pool.get();
        const t0 = Date.now();
        const raw = await g.conn.write('/ping', ['=address=' + ip, '=count=3', '=size=56']);
        res.json({
            success: true, ip, elapsedMs: Date.now() - t0,
            parsed: parsePingResult(raw, 3),
            rawIsArray: Array.isArray(raw),
            rawLength: Array.isArray(raw) ? raw.length : null,
            raw
        });
    } catch (e) { res.status(500).json({ success: false, ip, error: e.message }); }
});

// ============================================================
//  📋 API: LOGS
// ============================================================
// GET /api/logs?limit=200&offset=0&type=problem&ip=10.2.8&since=timestamp
app.get('/api/logs', (req, res) => {
    const limit = Math.min(Math.max(parseInt(req.query.limit) || 200, 1), 2000);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);
    const typeFilter = req.query.type || '';
    const ipFilter = String(req.query.ip || '').toLowerCase().trim();
    const since = parseInt(req.query.since) || 0;

    let filtered = LOGS;
    if (typeFilter && typeFilter !== 'all') {
        filtered = filtered.filter(l => l.type === typeFilter);
    }
    if (ipFilter) {
        filtered = filtered.filter(l => l.ip.toLowerCase().includes(ipFilter));
    }
    if (since > 0) {
        filtered = filtered.filter(l => l.timestamp >= since);
    }

    // Terbaru dulu
    const sorted = filtered.slice().sort((a, b) => b.timestamp - a.timestamp);
    const total = sorted.length;
    const page = sorted.slice(offset, offset + limit);

    res.json({
        success: true,
        total,
        offset,
        limit,
        count: page.length,
        logs: page
    });
});

// GET /api/logs/stats?since=timestamp — top device bermasalah
app.get('/api/logs/stats', (req, res) => {
    const since = parseInt(req.query.since) || 0;
    const filtered = since > 0 ? LOGS.filter(l => l.timestamp >= since) : LOGS;

    const byIp = new Map();
    let totalProblem = 0, totalWarning = 0, totalRecovery = 0;

    for (const l of filtered) {
        if (l.type === 'problem') totalProblem++;
        else if (l.type === 'warning') totalWarning++;
        else if (l.type === 'recovery') totalRecovery++;

        if (l.type !== 'problem' && l.type !== 'warning') continue;

        const key = l.ip;
        if (!byIp.has(key)) {
            byIp.set(key, {
                ip: l.ip,
                deviceName: l.deviceName || '',
                problem: 0,
                warning: 0,
                lastEvent: 0
            });
        }
        const s = byIp.get(key);
        if (l.type === 'problem') s.problem++;
        else if (l.type === 'warning') s.warning++;
        if (l.timestamp > s.lastEvent) s.lastEvent = l.timestamp;
        if (l.deviceName && !s.deviceName) s.deviceName = l.deviceName;
    }

    // Update nama dari DEVICES kalau ada
    for (const [ip, s] of byIp) {
        const d = DEVICES.find(x => x.ip === ip);
        if (d && d.deviceName) s.deviceName = d.deviceName;
    }

    const top = [...byIp.values()]
        .sort((a, b) => (b.problem + b.warning) - (a.problem + a.warning))
        .slice(0, 50);

    res.json({
        success: true,
        since,
        totalEvents: filtered.length,
        totalProblem,
        totalWarning,
        totalRecovery,
        totalDevices: byIp.size,
        top
    });
});

// DELETE /api/logs — hapus semua
app.delete('/api/logs', (req, res) => {
    const before = LOGS.length;
    LOGS = [];
    logDirty = true;
    const saved = saveLogsNow();
    if (!saved.ok) return res.status(500).json({ success: false, message: saved.message });
    console.log(`🗑️  Semua log dihapus (${before} entri)`);
    res.json({ success: true, removed: before });
});

// DELETE /api/logs/before/:timestamp — hapus yang lebih tua
app.delete('/api/logs/before/:timestamp', (req, res) => {
    const ts = parseInt(req.params.timestamp) || 0;
    if (ts <= 0) return res.status(400).json({ success: false, message: 'timestamp tidak valid' });
    const before = LOGS.length;
    LOGS = LOGS.filter(l => l.timestamp >= ts);
    const removed = before - LOGS.length;
    logDirty = true;
    const saved = saveLogsNow();
    if (!saved.ok) return res.status(500).json({ success: false, message: saved.message });
    console.log(`🗑️  Hapus log lebih tua dari ${new Date(ts).toISOString()} — ${removed} entri`);
    res.json({ success: true, removed, remaining: LOGS.length });
});

// DELETE /api/logs/ip/:ip — hapus log satu device
app.delete('/api/logs/ip/:ip', (req, res) => {
    const ip = String(req.params.ip || '').trim();
    if (!ip) return res.status(400).json({ success: false, message: 'IP wajib' });
    const before = LOGS.length;
    LOGS = LOGS.filter(l => l.ip !== ip);
    const removed = before - LOGS.length;
    logDirty = true;
    const saved = saveLogsNow();
    if (!saved.ok) return res.status(500).json({ success: false, message: saved.message });
    console.log(`🗑️  Hapus ${removed} log untuk ${ip}`);
    res.json({ success: true, removed, remaining: LOGS.length });
});

// ============================================================
//  ⚡ API: SCAN SUBNET
// ============================================================
app.post('/api/scan/start', (req, res) => {
    const { prefix, start = 1, end = 254 } = req.body || {};
    if (!prefix || !/^\d+\.\d+\.\d+$/.test(String(prefix))) {
        return res.status(400).json({ success: false, message: 'Prefix tidak valid' });
    }
    const s = Math.max(1, Math.min(254, parseInt(start) || 1));
    const e = Math.max(s, Math.min(254, parseInt(end) || 254));

    const scanId = makeScanId();
    const scan = {
        scanId, prefix: String(prefix), start: s, end: e,
        total: e - s + 1, scanned: 0, found: 0, online: [],
        done: false, running: false, aborted: false,
        startedAt: Date.now(), finishedAt: 0
    };
    SCANS.set(scanId, scan);
    console.log(`⚡ Scan dimulai: ${prefix}.${s} — ${prefix}.${e} (${scan.total} IP)`);
    runScan(scan).catch(e => console.error('❌ runScan error:', e.message));
    res.json({ success: true, scanId, total: scan.total });
});

app.get('/api/scan/status/:scanId', (req, res) => {
    const scan = SCANS.get(req.params.scanId);
    if (!scan) return res.status(404).json({ success: false, message: 'Scan tidak ditemukan' });
    res.json({
        success: true,
        scanId: scan.scanId, prefix: scan.prefix,
        start: scan.start, end: scan.end,
        total: scan.total, scanned: scan.scanned, found: scan.found,
        done: scan.done, aborted: !!scan.aborted,
        elapsedMs: (scan.finishedAt || Date.now()) - scan.startedAt,
        online: scan.done ? scan.online : scan.online.slice(-50)
    });
});

app.post('/api/scan/abort/:scanId', (req, res) => {
    const scan = SCANS.get(req.params.scanId);
    if (!scan) return res.status(404).json({ success: false });
    scan.aborted = true;
    console.log(`🛑 Scan ${scan.scanId} di-abort`);
    res.json({ success: true });
});

app.post('/api/scan/arp', async (req, res) => {
    const { prefix } = req.body || {};
    if (!prefix || !/^\d+\.\d+\.\d+$/.test(String(prefix))) {
        return res.status(400).json({ success: false, message: 'Prefix tidak valid' });
    }
    const t0 = Date.now();
    try {
        const g = await pool.get();
        const conn = g.conn;
        let arp = [];
        try { arp = await conn.write('/ip/arp/print'); } catch (e) {}
        let leases = [];
        try { leases = await conn.write('/ip/dhcp-server/lease/print'); } catch (e) {}

        const ips = new Map();
        const pfx = String(prefix) + '.';
        for (const a of (arp || [])) {
            const ip = a.address || '';
            if (!ip.startsWith(pfx)) continue;
            ips.set(ip, { ip, mac: a['mac-address'] || '', interface: a.interface || '', source: 'arp' });
        }
        for (const l of (leases || [])) {
            const ip = l.address || '';
            if (!ip.startsWith(pfx)) continue;
            const existing = ips.get(ip) || { ip, mac: '', interface: '', source: 'dhcp' };
            existing.mac = l['mac-address'] || existing.mac;
            existing.hostname = l.hostname || l.comment || '';
            existing.status = l.status || '';
            existing.source = existing.source === 'arp' ? 'both' : 'dhcp';
            ips.set(ip, existing);
        }
        const online = [...ips.values()].sort((a, b) => ipToNum(a.ip) - ipToNum(b.ip));
        console.log(`⚡ ARP scan ${prefix}.1-254: ${online.length} device dalam ${Date.now() - t0}ms`);
        res.json({ success: true, prefix, found: online.length, elapsedMs: Date.now() - t0, online });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ============================================================
//  💾  API: DEVICE DATABASE
// ============================================================
app.get('/api/devices', (req, res) => {
    const now = Date.now();
    const out = DEVICES.map(d => {
        const win = d.recentResults || [];
        const total = win.length;
        const successCount = win.reduce((a, b) => a + b, 0);
        const successRate = total > 0 ? Math.round((successCount / total) * 100) : null;
        const durationBase = getDurationBase(d, now);
        const durationType = getDurationType(d);
        const durationMs = Math.max(0, now - durationBase);
        return {
            ip: d.ip, deviceName: d.deviceName, status: d.status,
            responseTime: d.responseTime, lastChecked: d.lastChecked,
            durationBase, durationType, durationMs,
            durationText: formatDuration(durationMs),
            statusSince: d.statusSince, lastOnlineAt: d.lastOnlineAt,
            successRate, windowSize: total, windowMax: HISTORY_SIZE,
            consecutiveSuccess: d.consecutiveSuccess || 0,
            consecutiveFailures: d.consecutiveFailures || 0
        };
    });
    res.json(out);
});

app.post('/api/devices', (req, res) => {
    const incoming = req.body;
    if (!Array.isArray(incoming)) {
        return res.status(400).json({ success: false, message: 'Format harus array' });
    }
    const beforeCount = DEVICES.length;
    const existingMap = new Map(DEVICES.map(d => [d.ip, d]));
    const newDevices = [];
    const now = Date.now();
    let addedCount = 0;

    incoming.forEach(item => {
        const ip = String(item.ip || '').trim();
        if (!ip) return;
        const existing = existingMap.get(ip);
        if (existing) {
            existing.deviceName = String(item.deviceName || '').trim();
            newDevices.push(existing);
        } else {
            addedCount++;
            newDevices.push({
                ip, deviceName: String(item.deviceName || '').trim(),
                status: 'Memeriksa...', responseTime: 'N/A', lastChecked: '-',
                lastOnlineAt: now, lastSuccessMs: 0,
                consecutiveFailures: 0, consecutiveSuccess: 0,
                statusSince: now, recentResults: []
            });
        }
    });

    DEVICES = newDevices;
    const saved = saveNow();
    if (!saved.ok) return res.status(500).json({ success: false, message: saved.message, count: DEVICES.length });
    console.log(`💾 Tersimpan: ${beforeCount} → ${DEVICES.length} (+${addedCount} baru)`);
    res.json({ success: true, count: DEVICES.length, added: addedCount, devices: DEVICES.map(d => ({ ip: d.ip, deviceName: d.deviceName })) });
});

app.delete('/api/devices/:ip', (req, res) => {
    const ip = String(req.params.ip || '').trim();
    if (!ip) return res.status(400).json({ success: false, message: 'IP wajib' });
    const before = DEVICES.length;
    DEVICES = DEVICES.filter(d => d.ip !== ip);
    const removed = before - DEVICES.length;
    const saved = saveNow();
    if (!saved.ok) return res.status(500).json({ success: false, message: saved.message });
    console.log(`🗑️  Hapus ${ip} — ${before} → ${DEVICES.length}`);
    res.json({ success: true, removed, count: DEVICES.length });
});

// ============================================================
//  🌐  API: STATUS SERVER
// ============================================================
app.get('/api/status', (req, res) => {
    res.json({
        apiStatus, apiLatencyMs, lastApiOkAt,
        devicesCount: DEVICES.length,
        logsCount: LOGS.length,
        maxLogs: CONFIG.MAX_LOGS,
        poolSize: CONFIG.POOL_SIZE, poolConnected: pool.connectedCount,
        pingCount: PING_COUNT, batchSize: PING_BATCH_SIZE,
        siagaMs: SIAGA_THRESHOLD_MS, historySize: HISTORY_SIZE,
        unstableFailRate: UNSTABLE_FAIL_RATE,
        fastRecoveryN: CONFIG.FAST_RECOVERY_N, fastOfflineN: CONFIG.FAST_OFFLINE_N,
        scanConcurrency: CONFIG.SCAN_CONCURRENCY,
        activeScans: SCANS.size,
        cycleCounter,
        timestamp: Date.now()
    });
});

// ============================================================
//  🚀  START
// ============================================================
app.listen(PORT, () => {
    console.log('════════════════════════════════════════════════');
    console.log('  📡 MIKROTIK PING MONITOR');
    console.log('════════════════════════════════════════════════');
    console.log(`  🌐  Buka:          http://localhost:${PORT}`);
    console.log(`  📄  Devices DB:    ${DATA_FILE}`);
    console.log(`  📋  Logs DB:       ${LOG_FILE}`);
    console.log(`  🔌  MikroTik:      ${MIKROTIK_CONFIG.user}@${MIKROTIK_CONFIG.host}:${MIKROTIK_CONFIG.port}`);
    console.log(`  ⏱️  Interval:      ${PING_INTERVAL_MS} ms`);
    console.log(`  🟡  Siaga:         > ${SIAGA_THRESHOLD_MS} ms`);
    console.log(`  🟠  Unstable:      down > ${Math.round(UNSTABLE_FAIL_RATE*100)}%`);
    console.log(`  🔴  Offline:       ${CONFIG.FAST_OFFLINE_N} gagal + ${OFFLINE_AFTER_MS/1000}s`);
    console.log(`  📋  Max logs:      ${CONFIG.MAX_LOGS} entri`);
    console.log('════════════════════════════════════════════════');

    loadDevicesFromFile();
    loadLogsFromFile();
    backgroundPingLoop();
});

function shutdown(sig) {
    console.log(`\n📴 ${sig} — menyimpan data & menutup koneksi...`);
    saveNow();
    if (logDirty) saveLogsNow();
    pool.closeAll().finally(() => process.exit(0));
    setTimeout(() => process.exit(0), 1500);
}
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', (e) => console.error('❌ uncaughtException:', e.message));
process.on('unhandledRejection', (e) => console.error('❌ unhandledRejection:', e && e.message ? e.message : e));