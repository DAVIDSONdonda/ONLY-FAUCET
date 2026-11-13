const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const ROOT = path.join(__dirname, 'public');
const jobs = new Map();
const queue = [];

function json(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    // API publique accessible depuis une extension web.
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
  });
  res.end(data);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let raw = ''; let size = 0;
    req.on('data', chunk => { size += chunk.length; if (size > 15 * 1024 * 1024) { reject(new Error('Payload trop volumineux')); req.destroy(); } else raw += chunk; });
    req.on('end', () => { try { resolve(JSON.parse(raw || '{}')); } catch { reject(new Error('JSON invalide')); } });
    req.on('error', reject);
  });
}
function serveFile(req, res) {
  const requested = req.url === '/' ? '/index.html' : req.url.split('?')[0];
  const file = path.normalize(path.join(ROOT, requested));
  if (!file.startsWith(ROOT)) return json(res, 403, { error: 'Interdit' });
  fs.readFile(file, (err, data) => {
    if (err) return json(res, 404, { error: 'Introuvable' });
    const ext = path.extname(file); const types = { '.html':'text/html; charset=utf-8', '.js':'text/javascript; charset=utf-8', '.css':'text/css; charset=utf-8' };
    res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream' }); res.end(data);
  });
}
const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') return json(res, 204, {});
  try {
    if (req.method === 'GET' && (req.url === '/health' || req.url === '/api/health')) {
      return json(res, 200, { ok: true, service: 'gemini-local-bridge', time: new Date().toISOString() });
    }
    if (req.method === 'POST' && req.url === '/api/analyze') {
      const body = await readBody(req);
      if (!body.prompt || !body.imageDataUrl) return json(res, 400, { error: 'prompt et imageDataUrl sont requis' });
      const id = crypto.randomUUID();
      jobs.set(id, { id, prompt: String(body.prompt), imageDataUrl: String(body.imageDataUrl), status: 'queued', createdAt: Date.now() });
      queue.push(id);
      return json(res, 202, { id, status: 'queued' });
    }
    if (req.method === 'GET' && req.url === '/api/next') {
      const id = queue.shift();
      if (!id) return json(res, 204, {});
      const job = jobs.get(id); job.status = 'processing';
      return json(res, 200, { id: job.id, prompt: job.prompt, imageDataUrl: job.imageDataUrl });
    }
    if (req.method === 'POST' && req.url.startsWith('/api/result/')) {
      const id = req.url.split('/').pop(); const body = await readBody(req); const job = jobs.get(id);
      if (!job) return json(res, 404, { error: 'Job introuvable' });
      job.status = body.error ? 'error' : 'done'; job.response = body.response || null; job.error = body.error || null; job.updatedAt = Date.now();
      return json(res, 200, { ok: true });
    }
    if (req.method === 'GET' && req.url.startsWith('/api/status/')) {
      const id = req.url.split('/').pop(); const job = jobs.get(id);
      if (!job) return json(res, 404, { error: 'Job introuvable' });
      return json(res, 200, { id, status: job.status, response: job.response || null, error: job.error || null });
    }
    if (req.method === 'GET') return serveFile(req, res);
    return json(res, 404, { error: 'Route introuvable' });
  } catch (e) { json(res, 400, { error: e.message }); }
});
server.listen(PORT, HOST, () => console.log(`Gemini Local Bridge: listening on ${HOST}:${PORT}`));
