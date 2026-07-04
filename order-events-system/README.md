# Order Events System — Week 1-4 Hands-On Project

A minimal but realistic event-driven system built in **Java 21 + Spring Boot 3
+ Spring Kafka**: **order-service** (producer) → Kafka/Redpanda →
**inventory-service** (consumer/producer) → **notification-service**
(consumer). Used throughout the 12-week roadmap (`Roadmap.md`) as the one
project that grows week over week.

Each service is a standalone Maven project (`pom.xml` + `src/main/java/...`).

## What it demonstrates
- Event-carried state transfer, keyed partitioning for per-customer ordering —
  all three topics (`orders.created`, `orders.created.dlq`, `inventory.result`)
  are explicitly created with 3 partitions (`NewTopic` beans), and every
  publish is keyed by `customer_id` end-to-end, so a given customer's events
  always land on the same partition at every hop
- Idempotent consumption (dedup by `event_id`, `ConcurrentHashMap`-backed for now)
- `ErrorHandlingDeserializer` + `DeadLetterPublishingRecoverer` on both
  consumers — retries with backoff (deserialization failures skip straight to
  recovery), then routes unprocessable events to `orders.created.dlq` /
  `inventory.result.dlq` instead of blocking the partition
- Spring Boot Actuator health groups (`/actuator/health/liveness` and
  `/readiness`) wired into Kubernetes startup/liveness/readiness probes
- Containerized services (multi-stage Maven → JRE Docker builds), Kubernetes
  manifests with probes + HPA
- A CI/CD pipeline skeleton using build → test → push → GitOps deploy.
  Build + test run on every push/PR; image publish and the deploy stage are
  gated behind a manual `workflow_dispatch` trigger (learning mode — run them
  on demand from the Actions tab)

## Run it locally with Docker Compose
```bash
docker compose up --build
```
This starts Redpanda, Redpanda Console (UI at http://localhost:8080),
order-service (API at http://localhost:8000), and both consumers. First build
will be slow (Maven downloads dependencies inside the Docker build); subsequent
builds are cached.

## API
`order-service` exposes one endpoint. Seed stock: `WIDGET-1` = 50, `WIDGET-2` = 5,
`WIDGET-3` = 0 (see `InventoryConsumer.java`). The HTTP response only confirms
the order was published to Kafka — whether inventory reserves or fails it
happens downstream, so watch the logs (below) or Redpanda Console
(http://localhost:8080) to see the actual outcome.

**`POST /orders`** — create an order
```bash
# Happy path — enough stock, expect InventoryReserved downstream
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-1", "sku": "WIDGET-1", "quantity": 2}'

# Failure path — insufficient stock, expect InventoryFailed (insufficient_stock)
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-2", "sku": "WIDGET-2", "quantity": 10}'

# Failure path — zero-stock SKU, same InventoryFailed outcome
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-3", "sku": "WIDGET-3", "quantity": 1}'
```
All three return `HTTP 200` with `{"status": "published", "event": {...}}` —
that's Kafka accepting the publish, not the inventory decision.

Run `./scripts/smoke-test.sh` to fire all three against a running stack and
check the API responds as expected (see the script's header comment for what
it does and doesn't cover).

Watch the logs to see the actual downstream outcome:
```bash
docker compose logs -f inventory-service notification-service
```
You should see the inventory service reserve or fail stock and publish a
result, and the notification service print a simulated confirmation/failure
email.

## Run it locally without Docker (fastest inner loop)
Requires JDK 21+ and Maven installed.
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
