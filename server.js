const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const ROOT = path.join(__dirname, 'public');

const jobs = new Map();
let extensionSocket = null;
let mobileSocket = null;

function sendJson(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
    return true;
  }
  return false;
}

function sendNextQueuedJob() {
  if (!mobileSocket || mobileSocket.readyState !== WebSocket.OPEN) {
    return;
  }

  for (const [id, job] of jobs.entries()) {
    if (job.status === 'queued') {
      job.status = 'processing';
      sendJson(mobileSocket, {
        type: 'new_job',
        id: job.id,
        prompt: job.prompt,
        imageDataUrl: job.imageDataUrl
      });
      console.log(`[WS] Tâche envoyée à Android : ${id}`);
      return;
    }
  }
}

const server = http.createServer((req, res) => {
  const requested = req.url === '/' ? '/index.html' : req.url.split('?')[0];

  if (requested === '/health' || requested === '/api/health') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'no-store'
    });
    res.end(JSON.stringify({
      ok: true,
      service: 'gemini-websocket-bridge',
      time: new Date().toISOString(),
      queuedJobs: [...jobs.values()].filter(job => job.status === 'queued').length
    }));
    return;
  }

  const file = path.normalize(path.join(ROOT, requested));
  if (!file.startsWith(ROOT)) {
    res.writeHead(403, {
      'Content-Type': 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify({ error: 'Interdit' }));
    return;
  }

  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404, {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*'
      });
      res.end(JSON.stringify({ error: 'Introuvable' }));
      return;
    }

    res.writeHead(200, {
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'no-cache'
    });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('[WS] Nouveau client connecté');
  ws.isAlive = true;

  // Utilisé par le heartbeat WebSocket du serveur.
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message.toString());

      // Le client doit envoyer ce message périodiquement.
      // Cela maintient une activité entrante sur Render Free.
      if (data.type === 'ping') {
        sendJson(ws, {
          type: 'pong',
          time: new Date().toISOString()
        });
        return;
      }

      if (data.type === 'register_extension') {
        if (extensionSocket && extensionSocket !== ws) {
          extensionSocket.close(1000, 'Nouvelle extension connectée');
        }
        extensionSocket = ws;
        console.log('[WS] Extension Chrome enregistrée');
        sendJson(ws, { type: 'registered', client: 'extension' });
        return;
      }

      if (data.type === 'register_mobile') {
        if (mobileSocket && mobileSocket !== ws) {
          mobileSocket.close(1000, 'Nouvelle application mobile connectée');
        }
        mobileSocket = ws;
        console.log('[WS] Application mobile Android enregistrée');
        sendJson(ws, { type: 'registered', client: 'mobile' });
        sendNextQueuedJob();
        return;
      }

      if (data.type === 'analyze_image') {
        if (!data.imageDataUrl) {
          sendJson(ws, {
            type: 'error',
            error: 'imageDataUrl est obligatoire'
          });
          return;
        }

        const id = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        const job = {
          id,
          prompt: data.prompt || '',
          imageDataUrl: data.imageDataUrl,
          status: 'queued',
          createdAt: new Date().toISOString()
        };

        jobs.set(id, job);
        console.log(`[WS] Nouvelle tâche reçue de l’extension : ${id}`);

        sendJson(ws, {
          type: 'job_accepted',
          id,
          status: job.status
        });
        sendNextQueuedJob();
        return;
      }

      if (data.type === 'submit_result') {
        const { id, response } = data;
        const job = jobs.get(id);

        if (!job) {
          sendJson(ws, {
            type: 'error',
            id,
            error: 'Tâche introuvable ou déjà supprimée'
          });
          return;
        }

        job.status = 'done';
        job.response = response;
        job.completedAt = new Date().toISOString();
        console.log(`[WS] Résultat reçu pour ${id}`);

        sendJson(extensionSocket, {
          type: 'job_result',
          id,
          response
        });

        // La tâche n’est plus nécessaire après l’envoi du résultat.
        jobs.delete(id);
        sendNextQueuedJob();
        return;
      }

      sendJson(ws, {
        type: 'error',
        error: `Type de message inconnu : ${data.type || 'absent'}`
      });
    } catch (error) {
      console.error('[WS] Erreur de traitement du message :', error.message);
      sendJson(ws, {
        type: 'error',
        error: 'Message JSON invalide'
      });
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

      // Une tâche en cours pourra être renvoyée au prochain mobile connecté.
      for (const job of jobs.values()) {
        if (job.status === 'processing') {
          job.status = 'queued';
        }
      }
    }
  });

  ws.on('error', (error) => {
    console.error('[WS] Erreur socket :', error.message);
  });
});

// Vérifie régulièrement que les connexions WebSocket sont encore vivantes.
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }

    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => {
  clearInterval(heartbeatInterval);
});

server.listen(PORT, HOST, () => {
  console.log(`Serveur WebSocket & HTTP démarré sur ${HOST}:${PORT}`);
});
