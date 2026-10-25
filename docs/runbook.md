# Runbook – exploitation et dépannage

Incidents réellement rencontrés pendant la mise au point (machine de dev Windows, Docker Desktop, ~40 conteneurs d'autres projets).
Chaque entrée : symptôme → diagnostic → correctif → ce qui a été changé dans le repo pour que ça ne se reproduise pas.

## Vérifier la santé en 30 secondes

```bash
docker compose ps                                              # tout Up/healthy, les *-init en Exited (0)
curl -s localhost:18090/actuator/prometheus | grep published   # compteur qui augmente ≈ 25/s à 20 lots/s
docker compose logs --tail 3 spark-streaming                   # "batch N : X transactions" datés de maintenant
curl -s localhost:18800/metrics                                # alerts_processed qui monte, alerts_failed ≈ 0
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fraud-analyst-agent                       # LAG petit et stable
```

## Incidents

### 1. `port is already allocated` au démarrage
Un autre projet occupe 8080/8000/8090/4040. **Correctif** : tous les ports hôte sont en 18xxx/14040 et surchargeables dans `.env` (`AGENTS_PORT=…`).

### 2. Docker répond `500 Internal Server Error` / `502 Bad Gateway`
Le moteur Docker Desktop sature (mémoire). **Correctif** : redémarrer Docker Desktop, arrêter les stacks inutiles ; la stack a besoin de ~7 Go. Des `deploy.resources.limits` bornent chaque service pour qu'un conteneur n'étouffe pas les autres.

### 3. L'agent reçoit des alertes mais n'en termine aucune ; Kafka dit `group has no active members`
Une enquête LLM durait 11 min sans `poll()` → exclusion du groupe (`max.poll.interval.ms` = 5 min par défaut) → `commit()` en échec → thread mort.
**Correctif** (`agents/consumer.py`) : pattern **pause / poll / resume** — l'enquête tourne dans un thread, le consommateur continue de poller partitions en pause ; `max_poll_interval_ms` relevé à 30 min en ceinture.

### 4. Enquête de 11 min, puis `504 DEADLINE_EXCEEDED`
Mesure brute via `curl` : `gemini-3.8-flash` répondait « OK » en **112 s**, `gemini-flash-latest` en 17 min, `gemini-3.1-flash-lite` en **0,4 s**. Le palier gratuit ne sert pas tous les modèles à la même vitesse.
**Correctif** : modèle par défaut `gemini-3.1-flash-lite`, retries bornés (`LLM_MAX_RETRIES=2`), timeout 120 s, chronométrage de chaque appel LLM et de chaque outil dans les logs (`LLM appel #n terminé en … ms`, `outil … : … ms`). Toujours mesurer l'API nue avant d'accuser le code.

### 5. `is_fraud_label` visible par le LLM
L'étiquette de simulation était retirée de l'alerte mais pas des lignes d'historique Cassandra renvoyées par l'outil. **Correctif** (`tools.py`) : filtrée dans l'outil, avec test.

### 6. Spark affiche 0-1 transaction par batch alors que le producteur tourne
Le producteur idempotent était coincé depuis des heures (`OUT_OF_ORDER_SEQUENCE_NUMBER` après un redémarrage de Kafka, buffer plein, retries infinis) ; `published_total` n'augmentait plus et `failed_total` restait à 0 car les envois n'échouaient jamais, ils n'aboutissaient pas.
**Correctif** : `docker compose restart producer` (nouvel identifiant de producteur). À surveiller par le **delta** de `fraudstream_producer_published_total`, pas par `failed_total`.

### 7. Kafka `unhealthy` après ~20 h ; outils admin en `TimeoutException`
Mono-broker KRaft, heap 1 Go par défaut, données dans `/tmp` du conteneur, pas de `restart`.
**Correctif** : `KAFKA_HEAP_OPTS=-Xmx1536m`, données sur le volume `kafka-data`, rétention 48 h, `restart: unless-stopped` sur tous les services.

### 8. Spark : `RpcTimeoutException … spark.executor.heartbeatInterval`, batches datés de plusieurs minutes
Driver `local[*]` à 1 Go en pauses GC après 19 h et un Kafka instable. **Correctif** : `SPARK_DRIVER_MEMORY=2g` (limite conteneur 3 Go), `docker compose restart spark-streaming` reprend depuis le checkpoint sans perte.

## Commandes utiles

```bash
# Sauter le backlog d'alertes (groupe inactif requis)
docker compose stop agents
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group fraud-analyst-agent --topic fraud.alerts --reset-offsets --to-latest --execute
docker compose up -d agents

# Piloter le producteur sans redémarrer
curl -X POST localhost:18090/api/producer/pause | resume | rate/5
curl -X POST "localhost:18090/api/producer/inject/IMPOSSIBLE_TRAVEL?customerId=C00042"

# Mesurer un modèle Gemini à nu
curl -s -w "\nHTTP %{http_code} en %{time_total}s" -X POST \
  "https://generativelanguage.googleapis.com/v1beta/models/$MODEL:generateContent?key=$KEY" \
  -H "Content-Type: application/json" -d '{"contents":[{"parts":[{"text":"Reponds: OK"}]}]}'
```

### 9. Spark ne garde que 0-4 transactions par batch alors que Kafka en reçoit ~115 (le plus sournois)
Seules les transactions des scénarios de fraude passaient. Cause : le générateur datait certains événements **dans le futur**
(`IMPOSSIBLE_TRAVEL` : 2ᵉ transaction à `now + 4 min`). Le watermark Spark = max(eventTime) − 2 min = `now + 2 min`,
donc toute transaction normale datée `now` était « en retard » et **jetée avant l'opérateur à état**. Les premiers batches
(watermark pas encore avancé) étaient pleins, puis plus rien — et la détection paraissait parfaite (100 % de fraudes dans les alertes).
**Correctif** : les scénarios se déroulent dans le passé immédiat et se terminent à `now` (test `noScenarioEverDatesAnEventInTheFuture`),
watermark élargi à 5 min. **Leçon** : en event-time, surveiller `eventTime.watermark` dans `query.lastProgress` et le nombre de
lignes jetées (`numRowsDroppedByWatermark`) dès qu'un flux "maigrit" sans raison.

### 10. `429 RESOURCE_EXHAUSTED … limit: 500 … per day … Free Tier`
Le quota gratuit Gemini est de **500 requêtes/jour/modèle** ; un rattrapage de backlog (≈ 2-3 appels par enquête) le vide en une heure.
**Correctif** (`consumer.py`) : budget quotidien d'enquêtes (`MAX_INVESTIGATIONS_PER_DAY=150`) au-delà duquel les alertes restent
dans Kafka (partitions en pause, adhésion au groupe conservée) ; sur 429, pause automatique de la durée indiquée par l'API
(`retryDelay`) ; `/metrics` expose `investigations_today`, `daily_budget`, `paused_until`. Le quota étant par modèle, changer
`GEMINI_MODEL` permet de continuer la démo le jour même.
