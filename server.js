const express = require('express');
const cors = require('cors');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;
const jobs = new Map();
const queue = [];

// Comme HCAPTCHA, l’API est publique et accessible depuis une extension web.
app.use(cors());
app.use(express.json({ limit: '15mb' }));

app.get('/', (req, res) => {
  res.json({
    service: 'gemini-local-bridge',
    status: 'ok',
    health: '/health'
  });
});

// Render peut utiliser /health comme health check ; /api/health reste disponible.
function healthCheck(req, res) {
  res.json({
    status: 'ok',
    service: 'gemini-local-bridge',
    timestamp: new Date().toISOString()
  });
}
app.get('/health', healthCheck);
app.get('/api/health', healthCheck);

app.post('/api/analyze', (req, res) => {
  const { prompt, imageDataUrl } = req.body || {};
  if (!prompt || !imageDataUrl) {
    return res.status(400).json({ error: 'prompt et imageDataUrl sont requis' });
  }

  const id = crypto.randomUUID();
  jobs.set(id, {
    id,
    prompt: String(prompt),
    imageDataUrl: String(imageDataUrl),
    status: 'queued',
    createdAt: Date.now()
  });
  queue.push(id);
  return res.status(202).json({ id, status: 'queued' });
});

app.get('/api/next', (req, res) => {
  const id = queue.shift();
  if (!id) return res.status(204).end();

  const job = jobs.get(id);
  if (!job) return res.status(404).json({ error: 'Job introuvable' });
  job.status = 'processing';
  return res.json({
    id: job.id,
    prompt: job.prompt,
    imageDataUrl: job.imageDataUrl
  });
});

app.post('/api/result/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job introuvable' });

  const body = req.body || {};
  job.status = body.error ? 'error' : 'done';
  job.response = body.response || null;
  job.error = body.error || null;
  job.updatedAt = Date.now();
  return res.json({ ok: true });
});

app.get('/api/status/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job introuvable' });

  return res.json({
    id: job.id,
    status: job.status,
    response: job.response || null,
    error: job.error || null
  });
});

app.use((req, res) => {
  res.status(404).json({ error: 'Route introuvable' });
});

app.use((err, req, res, next) => {
  if (res.headersSent) return next(err);
  const status = err.status === 413 ? 413 : 400;
  const message = status === 413 ? 'Payload trop volumineux' : 'JSON invalide';
  return res.status(status).json({ error: message });
});

// app.listen(PORT) écoute sur toutes les interfaces, comme le serveur HCAPTCHA.
app.listen(PORT, () => {
  console.log(`Gemini Local Bridge démarré sur le port ${PORT}`);
});
