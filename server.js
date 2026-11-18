const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const ROOT = path.join(__dirname, 'public');

// Stockage en mémoire
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
  
  // Fichiers statiques éventuels
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

// Création du serveur WebSocket par-dessus HTTP
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('[WS] Nouveau client connecté');

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      // 1. L'extension s'enregistre et envoie une nouvelle tâche avec l'image
      if (data.type === 'register_extension') {
        extensionSocket = ws;
        console.log('[WS] Extension Chrome enregistrée');
      }
      
      // 2. L'application mobile s'enregistre
      else if (data.type === 'register_mobile') {
        mobileSocket = ws;
        console.log('[WS] Application mobile Android enregistrée');
        // S'il y a une tâche en attente, on l'envoie tout de suite au mobile
        for (const [id, job] of jobs.entries()) {
          if (job.status === 'queued') {
            job.status = 'processing';
            ws.send(JSON.stringify({ type: 'new_job', id: job.id, imageDataUrl: job.imageDataUrl }));
            break;
          }
        }
      }

      // 3. L'extension crée une tâche d'analyse
      else if (data.type === 'analyze_image') {
        const id = 'job_' + Date.now();
        const job = { id, prompt: data.prompt, imageDataUrl: data.imageDataUrl, status: 'queued' };
        jobs.set(id, job);
        console.log(`[WS] Nouvelle tâche reçue de l'extension : ${id}`);

        // Transmettre immédiatement au mobile s'il est connecté
        if (mobileSocket && mobileSocket.readyState === WebSocket.OPEN) {
          job.status = 'processing';
          mobileSocket.send(JSON.stringify({ type: 'new_job', id: job.id, imageDataUrl: job.imageDataUrl }));
        }
      }

      // 4. Le mobile renvoie le résultat de la résolution
      else if (data.type === 'submit_result') {
        const { id, response } = data;
        const job = jobs.get(id);
        if (job) {
          job.status = 'done';
          job.response = response;
          console.log(`[WS] Résultat reçu pour ${id} :`, response);

          // Transmettre instantanément la réponse à l'extension Chrome
          if (extensionSocket && extensionSocket.readyState === WebSocket.OPEN) {
            extensionSocket.send(JSON.stringify({ type: 'job_result', id, response }));
          }
          jobs.delete(id); // Nettoyage
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
