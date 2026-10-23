# Notes pour l'entretien (Big Data Engineer – GCP / Kafka / Spark-Scala / K8s / Azure)

Objectif : pouvoir raconter chaque brique en 1 minute, avec un choix, une alternative et une limite assumée.
Les réponses « je ne sais pas encore, voilà comment je le mesurerais » valent mieux qu'un chiffre inventé.

## Pitch (30 s)

« J'ai construit une plateforme de détection de fraude temps réel de bout en bout : un producteur Spring Boot publie
des transactions dans Kafka, un job Spark Structured Streaming en Scala maintient un état par client et score chaque
transaction, les résultats vont dans Cassandra pour le serving, dans un lac Parquet puis BigQuery via Airflow pour
l'analytique, et chaque alerte déclenche un agent IA (Gemini + LangGraph) qui enquête avec des outils et produit un
rapport auditable. Tout tourne en local avec Docker, se déploie sur Kubernetes avec kustomize, et j'ai documenté la
portabilité Azure. »

## Questions probables et réponses courtes

**Pourquoi `flatMapGroupsWithState` plutôt que des agrégations fenêtrées ?**
Parce que les règles ont besoin d'un contexte par client qui dépasse une fenêtre (devices connus, moyenne historique,
dernier pays) et qu'une transaction doit sortir immédiatement avec son score. L'état est borné (fenêtre 10 min, 10 devices,
moyenne incrémentale) et expire 24 h après la dernière transaction (timeout en temps événement). RocksDB pour l'état
car la heap du driver/executor ne doit pas dépendre du nombre de clients.

**Comment garantis-tu l'ordre des transactions d'un client ?**
Clé Kafka = customerId ⇒ même partition ⇒ ordre garanti. Dans le groupe, je trie quand même par `eventTime` dans le
micro-batch. Si un client est très actif (« hot key »), c'est la limite du design : on peut sous-partitionner par
(customerId, carte) ou passer à un état plus fin.

**Exactly-once ?**
Lac Parquet : oui (checkpoint + commit log). Cassandra : écritures idempotentes (clé = transactionId), donc rejouer
un batch ne crée pas de doublon. Kafka alerts : at-least-once, l'agent dédoublonne sur `alertId` déterministe.
Le point faible assumé : foreachBatch avec plusieurs sinks n'est pas atomique entre sinks ; si Cassandra tombe
après l'écriture du lac, le batch est rejoué et Cassandra rattrape, le lac est idempotent grâce au commit log.

**Pourquoi Cassandra ET BigQuery ?**
Patterns d'accès différents : lookup des 20 dernières transactions d'un client en quelques ms (agent, écran analyste)
vs scans analytiques sur des mois. BigQuery n'est pas fait pour le point lookup, Cassandra pas pour l'agrégation ad hoc.
Modélisation query-first : une table par requête (`transactions_by_customer`, `alerts_by_day`, `customer_profile`).

**Comment maîtrises-tu les coûts BigQuery ?**
Partition par jour + clustering, expiration des partitions, chargement batch depuis le lac (pas de streaming inserts),
`maximum_bytes_billed` sur chaque requête générée par le LLM, et un budget d'alerte GCP. Toute la démo tient dans le
palier gratuit.

**Et Azure ?**
Même architecture avec Event Hubs (API Kafka : seule la config change), Databricks ou Spark sur AKS (même jar),
Cosmos DB API Cassandra, ADLS Gen2, Synapse/Fabric, Azure OpenAI. Détail dans `docs/azure-portability.md`.

**Que fait concrètement l'agent IA, et pourquoi ce n'est pas un gadget ?**
Il remplace la première passe de l'analyste : lire l'alerte, vérifier l'historique et le profil du client, comparer à la
population, rendre un verdict et une action. Il est en lecture seule, ses requêtes SQL passent par un parseur
(`sqlglot`) qui refuse tout sauf `SELECT` sur des tables autorisées avec un `LIMIT`, chaque appel d'outil est tracé,
et une sortie mal formée donne un verdict prudent (`MONITOR`), jamais un blocage de carte. L'étiquette de simulation
`isFraudLabel` est retirée du contexte : sinon l'évaluation serait biaisée.

**Comment évalues-tu la détection ?**
Les scénarios injectés sont étiquetés ⇒ précision/rappel global et rappel par scénario calculés chaque jour par le DAG.
Le tableau `scenario_recall` montre par exemple que `HIGH_AMOUNT` n'est pas alerté (0.55 < 0.6) : c'est volontaire,
pour montrer comment la boucle mesure → règle/modèle fonctionne. Étape suivante : modèle entraîné sur les features
déjà calculées en streaming, branché dans `RiskScorer.process` sans toucher au reste.

**Kubernetes : qu'est-ce qui est prod-ready et qu'est-ce qui ne l'est pas ?**
Prod-ready : kustomize base/overlays, StatefulSets avec PVC pour Kafka/Cassandra, probes, resources, Workload Identity
(aucune clé JSON dans le cluster). Pas prod-ready, et je le dis : mono-broker KRaft (Strimzi en prod), Cassandra
1 réplica, Spark en `local[*]` dans un pod (script `spark-submit` cluster fourni), pas de NetworkPolicy ni PodDisruptionBudget.

**Scala : à l'aise ?**
Le moteur de scoring est en Scala pur et testé sans Spark (ScalaTest) ; le job utilise l'API Dataset typée avec des
case classes et des encodeurs. Je connais les pièges : ambiguïté `foreachBatch` en 2.12, `--add-opens` sur Java 17,
`Provided` pour les dépendances Spark dans le fat-jar, stratégies de merge sbt-assembly.

## Chiffres à mesurer avant l'entretien (à reporter dans le README)

- Débit max soutenu en local sans lag croissant (monter `rate` par paliers : 20, 100, 500 lots/s).
- Latence Kafka → alerte (différence `eventTime` → `created_at` de l'enquête, p50/p95).
- Précision / rappel du jour et rappel par scénario après 1 h de run.
- Latence moyenne et nombre d'appels d'outils par enquête Gemini.

## Ce que j'ai appris / ce que je referais autrement

- Un schéma JSON partagé entre Java et Scala sans registry, ça marche en POC mais c'est fragile : Avro + Schema Registry dès le départ.
- Les tests sur `MemoryStream` sont précieux : ils valident watermark, état et encodeurs sans Kafka.
- Mettre les garde-fous de l'agent dans une couche séparée et testée (pas dans le prompt) est ce qui rend l'IA acceptable par un RSSI.
- Piège Scala 2.12 réel rencontré en revue : `Iterator.toSeq` renvoie un `Stream` paresseux ; dans `flatMapGroupsWithState`,
  un `map` qui met à jour l'état n'était évalué que pour la première transaction du groupe → `state.update` persistait
  un état partiel. Corrigé par `toList` (strict). Bonne anecdote sur « pourquoi les tests MemoryStream ne suffisent pas ».
