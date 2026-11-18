const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const ROOT = path.join(__dirname, 'public');

const jobs = new Map();
let extensionSocket = null;
let mobileSocket = null;

const server = http.createServer((req, res) => {
  const requested = req.url === '/' ? '/index.html' : req.url.split('?')[0];
  if (requested === '/health' || requested === '/api/health') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, service: 'gemini-websocket-bridge', time: new Date().toISOString() }));
    return;
  }
  
  const file = path.normalize(path.join(ROOT, requested));
  if (!file.startsWith(ROOT)) {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Interdit' }));
    return;
  }
  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Introuvable' }));
      return;
    }
    res.writeHead(200);
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('[WS] Nouveau client connecté');

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'register_extension') {
        extensionSocket = ws;
        console.log('[WS] Extension Chrome enregistrée');
      }
      else if (data.type === 'register_mobile') {
        mobileSocket = ws;
        console.log('[WS] Application mobile Android enregistrée');
        for (const [id, job] of jobs.entries()) {
          if (job.status === 'queued') {
            job.status = 'processing';
            ws.send(JSON.stringify({ type: 'new_job', id: job.id, imageDataUrl: job.imageDataUrl }));
            break;
          }
        }
      }
      else if (data.type === 'analyze_image') {
        const id = 'job_' + Date.now();
        const job = { id, prompt: data.prompt, imageDataUrl: data.imageDataUrl, status: 'queued' };
        jobs.set(id, job);
        console.log(`[WS] Nouvelle tâche reçue de l'extension : ${id}`);

        if (mobileSocket && mobileSocket.readyState === WebSocket.OPEN) {
          job.status = 'processing';
          mobileSocket.send(JSON.stringify({ type: 'new_job', id: job.id, imageDataUrl: job.imageDataUrl }));
        }
      }
      else if (data.type === 'submit_result') {
        const { id, response } = data;
        const job = jobs.get(id);
        if (job) {
          job.status = 'done';
          job.response = response;
          console.log(`[WS] Résultat reçu pour ${id} :`, response);

          if (extensionSocket && extensionSocket.readyState === WebSocket.OPEN) {
            extensionSocket.send(JSON.stringify({ type: 'job_result', id, response }));
          }
          jobs.delete(id);
        }
      }
    } catch (e) {
      console.error('[WS] Erreur de traitement du message :', e.message);
    }
  });

  ws.on('close', () => {
    if (ws === extensionSocket) {
      extensionSocket = null;
      console.log('[WS] Extension Chrome déconnectée');
    }
    if (ws === mobileSocket) {
      mobileSocket = null;
      console.log('[WS] Application mobile déconnectée');
    }
  });
});

server.listen(PORT, HOST, () => {
  console.log(`Serveur WebSocket & HTTP démarré sur ${HOST}:${PORT}`);
});
