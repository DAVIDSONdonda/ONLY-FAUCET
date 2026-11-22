# ONLY-FAUCET

API publique Node.js pour le pont Gemini Local Bridge, avec choix du processeur mobile ou extension navigateur.

## Déploiement sur Render

Créer un service **Web Service** relié à ce dépôt et utiliser :

- **Build Command** : `npm install`
- **Start Command** : `npm start`
- **Health Check Path** : `/health`

Le serveur utilise automatiquement le port fourni par Render via `PORT`. Aucun jeton n’est requis : l’API est publique et accepte les appels cross-origin pour pouvoir être utilisée par une extension web.

## Routes

- `GET /health` et `GET /api/health` : état du serveur
- `POST /api/analyze` : ajouter un travail avec `prompt`, `imageDataUrl` et `processingTarget` (`mobile` ou `extension`)
- `GET /api/next` : récupérer le prochain travail
- `POST /api/result/:id` : transmettre le résultat d’un travail
- `GET /api/status/:id` : consulter l’état d’un travail

## Clients WebSocket

- `register_extension` : client qui crée une tâche et reçoit `job_result`.
- `register_mobile` : worker mobile recevant les tâches `processingTarget: "mobile"`.
- `register_browser_worker` : worker Gemini/Claude recevant les tâches `processingTarget: "extension"`.

## Limites à connaître

Les travaux sont conservés en mémoire, comme dans la version actuelle du serveur HCAPTCHA : ils sont perdus au redémarrage ou au redéploiement. Toutes les routes sont publiques ; toute personne connaissant l’URL peut envoyer, récupérer ou modifier des travaux. Ne pas y transmettre de données confidentielles sans ajouter une authentification et une base de données adaptées.
