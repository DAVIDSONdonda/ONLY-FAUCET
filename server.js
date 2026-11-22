const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const ROOT = path.join(__dirname, 'public');
const JOB_TTL_MS = 2 * 60 * 1000;

// Les Map conservent l’ordre d’insertion : la première tâche est toujours servie en premier.
const jobs = new Map();
const mobileClients = new Map();
const browserWorkers = new Map();
const extensionClients = new Set();
const expiredJobTargets = new Map();

let nextMobileNumber = 1;

function sendJson(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
    return true;
  }
  return false;
}

function activeJobExists() {
  for (const job of jobs.values()) {
    if (job.status === 'processing') return true;
  }
  return false;
}

function getFirstQueuedJob() {
  for (const job of jobs.values()) {
    if (job.status === 'queued') return job;
  }
  return null;
}

function getFreeWorker(pool) {
  for (const client of pool.values()) {
    if (client.ws.readyState === WebSocket.OPEN && client.currentJobId === null && client.awaitingReload === false) return client;
  }
  return null;
}
function getFreeMobile() {
  return getFreeWorker(mobileClients);
}

// Traitement volontairement séquentiel : une seule tâche active à la fois.
function dispatchNextJob() {
  if (activeJobExists()) return;

  const job = getFirstQueuedJob();
  if (!job) return;

  const pool = job.processingTarget === 'extension' ? browserWorkers : mobileClients;
  const worker = getFreeWorker(pool);
  if (!worker) return;

  job.status = 'processing';
  job.workerId = worker.id;
  job.workerSocket = worker.ws;
  job.startedAt = new Date().toISOString();
  worker.currentJobId = job.id;

  const sent = sendJson(worker.ws, {
    type: 'new_job',
    id: job.id,
    prompt: job.prompt,
    imageDataUrl: job.imageDataUrl,
    queuePosition: 1,
    processingTarget: job.processingTarget
  });

  if (!sent) {
    job.status = 'queued';
    job.workerId = null;
    job.workerSocket = null;
    job.startedAt = null;
    worker.currentJobId = null;
    return;
  }

  console.log(`[QUEUE] Tâche ${job.id} envoyée à ${worker.id} (${job.processingTarget})`);
}

function queueStats() {
  let queued = 0;
  let processing = 0;
  for (const job of jobs.values()) {
    if (job.status === 'queued') queued++;
    if (job.status === 'processing') processing++;
  }
  return {
    queued,
    processing,
    connectedMobiles: mobileClients.size,
    connectedBrowserWorkers: browserWorkers.size,
    connectedExtensions: extensionClients.size
  };
}

function expireJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) return;

  if (job.extensionSocket && job.extensionTabId != null) {
    const target = {
      socket: job.extensionSocket,
      tabId: job.extensionTabId
    };
    expiredJobTargets.set(job.id, target);
    setTimeout(() => {
      if (expiredJobTargets.get(job.id) === target) {
        expiredJobTargets.delete(job.id);
      }
    }, 5 * 60 * 1000);
  }

  const pool = job.processingTarget === 'extension' ? browserWorkers : mobileClients;
  const worker = job.workerId ? pool.get(job.workerId) : null;
  if (worker && worker.currentJobId === job.id) {
    worker.currentJobId = null;
    worker.awaitingReload = true;
  }

  sendJson(job.extensionSocket, {
    type: 'job_expired',
    id: job.id,
    error: 'La tâche a expiré après 2 minutes.'
  });

  sendJson(job.workerSocket, {
    type: 'job_expired',
    id: job.id,
    error: 'La tâche a expiré après 2 minutes.'
  });

  console.log(`[QUEUE] Tâche ${job.id} expirée après 2 minutes`);
  jobs.delete(job.id);
  dispatchNextJob();
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
      service: 'gemini-websocket-queue-bridge',
      time: new Date().toISOString(),
      ...queueStats()
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
  ws.clientType = null;
  ws.mobileId = null;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message.toString());

      if (data.type === 'ping') {
        sendJson(ws, {
          type: 'pong',
          time: new Date().toISOString()
        });
        return;
      }

      if (data.type === 'register_extension') {
        ws.clientType = 'extension';
        extensionClients.add(ws);
        console.log(`[WS] Extension enregistrée (${extensionClients.size} connectées)`);
        sendJson(ws, {
          type: 'registered',
          client: 'extension',
          queue: queueStats()
        });
        return;
      }

      if (data.type === 'register_browser_worker') {
        const workerId = `browser_${browserWorkers.size + 1}_${Date.now()}`;
        const worker = { id: workerId, ws, currentJobId: null, awaitingReload: false, connectedAt: new Date().toISOString() };
        ws.clientType = 'browser_worker';
        ws.workerId = workerId;
        browserWorkers.set(workerId, worker);
        console.log(`[WS] ${workerId} enregistré (${browserWorkers.size} extensions de traitement)`);
        sendJson(ws, { type: 'registered', client: 'browser_worker', workerId, queue: queueStats() });
        dispatchNextJob();
        return;
      }
      if (data.type === 'register_mobile') {
        const mobileId = `mobile_${nextMobileNumber++}`;
        const mobile = {
          id: mobileId,
          ws,
          currentJobId: null,
          awaitingReload: false,
          connectedAt: new Date().toISOString()
        };

        ws.clientType = 'mobile';
        ws.mobileId = mobileId;
        mobileClients.set(mobileId, mobile);

        console.log(`[WS] ${mobileId} enregistré (${mobileClients.size} appareils connectés)`);
        sendJson(ws, {
          type: 'registered',
          client: 'mobile',
          mobileId,
          queue: queueStats()
        });

        dispatchNextJob();
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
          processingTarget: data.processingTarget === 'extension' ? 'extension' : 'mobile',
          extensionSocket: ws,
          extensionTabId: Number.isInteger(data.tabId) ? data.tabId : null,
          mobileSocket: null,
          mobileId: null,
          workerSocket: null,
          workerId: null,
          createdAt: new Date().toISOString(),
          startedAt: null,
          expireTimer: null
        };

        jobs.set(id, job);
        job.expireTimer = setTimeout(() => expireJob(id), JOB_TTL_MS);
        console.log(`[QUEUE] Nouvelle tâche ${id}. File : ${queueStats().queued}`);

        sendJson(ws, {
          type: 'job_accepted',
          id,
          status: 'queued',
          queue: queueStats()
        });

        dispatchNextJob();
        return;
      }

      if (data.type === 'submit_result') {
        const { id, response } = data;
        const job = jobs.get(id);

        if (!job) {
          sendJson(ws, {
            type: 'error',
            id,
            error: 'Tâche introuvable ou déjà terminée'
          });
          return;
        }

        if (job.workerSocket !== ws) {
          sendJson(ws, {
            type: 'error',
            id,
            error: 'Cette tâche est attribuée à un autre appareil'
          });
          return;
        }

        job.status = 'done';
        job.response = response || '';
        job.completedAt = new Date().toISOString();
        clearTimeout(job.expireTimer);

        const pool = job.processingTarget === 'extension' ? browserWorkers : mobileClients;
        const worker = pool.get(job.workerId);
        if (worker && worker.currentJobId === id) worker.currentJobId = null;

        // Le résultat est envoyé uniquement à l’extension qui a créé la tâche.
        sendJson(job.extensionSocket, {
          type: 'job_result',
          id,
          response: job.response
        });

        console.log(`[QUEUE] Tâche ${id} terminée par ${job.workerId} (${job.processingTarget})`);
        jobs.delete(id);

        // La tâche suivante est envoyée immédiatement au prochain appareil libre.
        dispatchNextJob();
        return;
      }

      if (data.type === 'reload_extension_page') {
        const target = expiredJobTargets.get(data.id);
        if (!target || target.socket.readyState !== WebSocket.OPEN) {
          sendJson(ws, {
            type: 'error',
            id: data.id,
            error: 'Extension introuvable pour cette tâche expirée'
          });
          return;
        }

        sendJson(target.socket, {
          type: 'reload_page',
          id: data.id,
          tabId: target.tabId
        });
        expiredJobTargets.delete(data.id);
        const workerPool = ws.clientType === 'browser_worker' ? browserWorkers : mobileClients;
        const worker = ws.workerId ? workerPool.get(ws.workerId) : null;
        if (worker) worker.awaitingReload = false;
        sendJson(ws, { type: 'reload_requested', id: data.id });
        dispatchNextJob();
        return;
      }

      sendJson(ws, {
        type: 'error',
        error: `Type de message inconnu : ${data.type || 'absent'}`
      });
    } catch (error) {
      console.error('[WS] Erreur de traitement :', error.message);
      sendJson(ws, {
        type: 'error',
        error: 'Message JSON invalide'
      });
    }
  });

  ws.on('close', () => {
    if (ws.clientType === 'extension') {
      extensionClients.delete(ws);
      for (const job of jobs.values()) {
        if (job.extensionSocket === ws) job.extensionSocket = null;
      }
      console.log(`[WS] Extension déconnectée (${extensionClients.size} restantes)`);
    }

    if (ws.clientType === 'browser_worker' && ws.workerId) {
      const worker = browserWorkers.get(ws.workerId);
      if (worker?.currentJobId) {
        const job = jobs.get(worker.currentJobId);
        if (job && job.status === 'processing') {
          job.status = 'queued'; job.workerId = null; job.workerSocket = null; job.startedAt = null;
          console.log(`[QUEUE] Tâche ${job.id} remise en file après déconnexion de ${ws.workerId}`);
        }
      }
      browserWorkers.delete(ws.workerId);
      console.log(`[WS] ${ws.workerId} déconnecté (${browserWorkers.size} extensions de traitement restantes)`);
      dispatchNextJob();
    }
    if (ws.clientType === 'mobile' && ws.mobileId) {
      const mobile = mobileClients.get(ws.mobileId);
      if (mobile?.currentJobId) {
        const job = jobs.get(mobile.currentJobId);
        if (job && job.status === 'processing') {
          job.status = 'queued';
          job.workerId = null;
          job.workerSocket = null;
          job.mobileId = null;
          job.mobileSocket = null;
          job.startedAt = null;
          console.log(`[QUEUE] Tâche ${job.id} remise en file après déconnexion de ${ws.mobileId}`);
        }
      }
      mobileClients.delete(ws.mobileId);
      console.log(`[WS] ${ws.mobileId} déconnecté (${mobileClients.size} restants)`);
      dispatchNextJob();
    }
  });

  ws.on('error', (error) => {
    console.error('[WS] Erreur socket :', error.message);
  });
});

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

wss.on('close', () => clearInterval(heartbeatInterval));

server.listen(PORT, HOST, () => {
  console.log(`Serveur WebSocket FIFO démarré sur ${HOST}:${PORT}`);
});
