# FraudStream – raccourcis. `make help` pour la liste.
SHELL := /bin/bash
PROJECT_ID ?=
REGION ?= europe-west1
TAG ?= 0.1.0

.PHONY: help up down logs ps demo inject test test-agents test-batch test-producer test-spark \
        build-images push-images kind-up kind-deploy kind-down gcp-setup airflow-run clean

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## Démarre toute la stack locale (docker compose)
	docker compose up -d --build

down: ## Arrête la stack (conserve les volumes)
	docker compose down

clean: ## Arrête la stack et supprime les volumes (données, checkpoints)
	docker compose down -v

logs: ## Suit les logs des services applicatifs
	docker compose logs -f producer spark-streaming agents

ps: ## État des conteneurs
	docker compose ps

demo: ## Scénario de démo : injecte 4 fraudes et affiche les enquêtes de l'agent
	./scripts/demo.sh

inject: ## Injecte un scénario : make inject S=IMPOSSIBLE_TRAVEL
	curl -s -X POST "http://localhost:8090/api/producer/inject/$(S)" && echo

airflow-run: ## Déclenche le DAG batch pour aujourd'hui
	docker compose exec airflow airflow dags trigger fraudstream_daily -e $$(date -u +%F)

# ------------------------------------------------------------------ tests
test: test-agents test-batch ## Tests Python exécutables sans Docker (agents + batch)

test-agents: ## Tests des agents (LLM simulé, DuckDB)
	cd agents && python -m pytest -q

test-batch: ## Tests du job PySpark batch
	cd batch-airflow && python -m pytest -q tests

test-producer: ## Tests du producteur Spring Boot (Maven)
	cd producer-springboot && mvn -q test

test-spark: ## Tests Scala (RiskScorer + pipeline MemoryStream)
	cd streaming-spark && sbt -batch test

# ------------------------------------------------------------------ images & kubernetes
build-images: ## Construit les 3 images applicatives
	docker build -t fraudstream/producer:$(TAG) producer-springboot
	docker build -t fraudstream/spark-streaming:$(TAG) streaming-spark
	docker build -t fraudstream/agents:$(TAG) agents

kind-up: ## Crée un cluster kind et y charge les images
	kind create cluster --config k8s/overlays/kind/kind-config.yaml
	kind load docker-image fraudstream/producer:$(TAG) fraudstream/spark-streaming:$(TAG) fraudstream/agents:$(TAG) --name fraudstream

kind-deploy: ## Déploie sur kind (copier d'abord k8s/overlays/kind/secret.example.yaml en secret.yaml)
	kubectl apply -k k8s/overlays/kind
	kubectl -n fraudstream get pods -w

kind-down: ## Supprime le cluster kind
	kind delete cluster --name fraudstream

push-images: ## Pousse les images vers Artifact Registry (PROJECT_ID requis)
	@test -n "$(PROJECT_ID)" || (echo "PROJECT_ID manquant" && exit 1)
	gcloud auth configure-docker $(REGION)-docker.pkg.dev --quiet
	for img in producer spark-streaming agents; do \
	  docker tag fraudstream/$$img:$(TAG) $(REGION)-docker.pkg.dev/$(PROJECT_ID)/fraudstream/$$img:$(TAG); \
	  docker push $(REGION)-docker.pkg.dev/$(PROJECT_ID)/fraudstream/$$img:$(TAG); \
	done

gcp-setup: ## Provisionne BigQuery, GCS, compte de service, budget (PROJECT_ID requis)
	PROJECT_ID=$(PROJECT_ID) ./gcp/setup.sh
