# Order Events System — Week 1-4 Hands-On Project

A minimal but realistic event-driven system built in **Java 17 + Spring Boot 3
+ Spring Kafka**: **order-service** (producer) → Kafka/Redpanda →
**inventory-service** (consumer/producer) → **notification-service**
(consumer). Used throughout the 12-week roadmap (`Roadmap.md`) as the one
project that grows week over week.

Each service is a standalone Maven project (`pom.xml` + `src/main/java/...`).
`producer/`, `consumer_inventory/`, and `consumer_notification/` still contain
stale `app.py`/`requirements.txt`/`__pycache__` files from an earlier Python
draft — they're unused, safe to delete, and can be ignored.

## What it demonstrates
- Event-carried state transfer, keyed partitioning for per-customer ordering
- Idempotent consumption (dedup by `event_id`, `ConcurrentHashMap`-backed for now)
- `ErrorHandlingDeserializer` + `DeadLetterPublishingRecoverer` — retries twice
  with backoff, then routes unprocessable events to `orders.created.dlq`
  instead of blocking the partition
- Spring Boot Actuator health groups (`/actuator/health/liveness` and
  `/readiness`) wired into Kubernetes startup/liveness/readiness probes
- Containerized services (multi-stage Maven → JRE Docker builds), Kubernetes
  manifests with probes + HPA
- A CI/CD pipeline skeleton using build → test → push → GitOps deploy

## Run it locally with Docker Compose
```bash
docker compose up --build
```
This starts Redpanda, Redpanda Console (UI at http://localhost:8080),
order-service (API at http://localhost:8000), and both consumers. First build
will be slow (Maven downloads dependencies inside the Docker build); subsequent
builds are cached.

Create an order:
```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-1", "sku": "WIDGET-1", "quantity": 2}'
```

Watch the logs:
```bash
docker compose logs -f inventory-service notification-service
```
You should see the inventory service reserve stock and publish a result, and
the notification service print a simulated email.

Try triggering a failure path — order more than available stock (`WIDGET-2`
only has 5 units), and watch `InventoryFailed` flow through instead.

## Run it locally without Docker (fastest inner loop)
Requires JDK 17 and Maven installed.
```bash
docker compose up redpanda redpanda-console   # just the broker
cd producer && mvn spring-boot:run             # in one terminal
cd consumer_inventory && mvn spring-boot:run   # in another
cd consumer_notification && mvn spring-boot:run
```

## Run it on Kubernetes (Week 5+)
Requires `kind` or `minikube` installed locally.
```bash
# build images locally
docker build -t order-service:local ./producer
docker build -t inventory-service:local ./consumer_inventory
docker build -t notification-service:local ./consumer_notification

# load into kind (skip this line if using minikube — use `minikube image load` instead)
kind load docker-image order-service:local inventory-service:local notification-service:local

kubectl apply -f k8s/
kubectl -n order-events get pods -w
```
Port-forward to hit the API:
```bash
kubectl -n order-events port-forward svc/order-service 8000:8000
```

## Where to go next (matches the Roadmap)
- Week 2: add a Postgres outbox table instead of the in-memory `processedEventIds` set
- Week 3: add a schema registry (Confluent/Apicurio) and stop duplicating the
  `OrderCreatedEvent`/`InventoryResultEvent` classes across services
- Week 6-7: add Ingress, a Helm chart, load-test the HPA
- Week 9-11: wire the GitHub Actions workflow to a real registry and ArgoCD

## Notes on production differences
- Redpanda here runs single-node with no persistence — real deployments use
  3+ broker StatefulSets with PVCs.
- Event classes (`OrderCreatedEvent`, `InventoryResultEvent`) are duplicated
  per service on purpose — that pain is what motivates a shared schema/contract
  in Week 3.
- The idempotency store and stock map are in-memory — replace with a real
  database before this goes anywhere near production.
- I couldn't run Maven/Docker in my own sandbox to execute this end-to-end
  (no outbound access to Maven Central and no Docker daemon there), so the
  Java source compiles cleanly by inspection but hasn't been built and run —
  worth doing a `docker compose up --build` on your machine as a first check.
