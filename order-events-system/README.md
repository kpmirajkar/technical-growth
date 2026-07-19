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
- Transactional outbox (Week 2): `POST /orders` writes the order row and its
  `OrderCreated` event to Postgres in one transaction; a scheduled
  `OutboxPublisher` relays pending rows to Kafka (at-least-once) — no dual
  write to DB + broker
- Idempotent consumption enforced by the database (Week 2): dedup via a
  `processed_events` primary key and an atomic conditional stock decrement
  (`UPDATE ... WHERE quantity >= ?`) — survives restarts, replicas, and
  concurrent redelivery, which the old in-memory `HashSet` could not
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
Seed stock: `WIDGET-1` = 50, `WIDGET-2` = 5, `WIDGET-3` = 0 (seeded by
`consumer_inventory`'s `data.sql`). Since the Week 2 outbox, `POST /orders`
returning 200 means the order and its event are **durably recorded in
Postgres** — the event reaches Kafka within ~500ms via the outbox relay, and
the inventory decision happens downstream. Watch the logs (below) or Redpanda
Console (http://localhost:8080) for the actual outcome.

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
All three return `HTTP 200` with `{"status": "accepted", "event": {...}}` —
that's the order durably persisted (outbox pending), not the inventory
decision.

**`GET /orders/{id}`** — fetch a persisted order (id comes from the POST
response's `event.order_id`)
```bash
curl http://localhost:8000/orders/<order_id>
# -> {"id":"...","customer_id":"cust-1","sku":"WIDGET-1","quantity":2,
#     "status":"PLACED","created_at":"..."}   (404 for unknown ids)
```

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
docker compose up redpanda redpanda-console postgres   # broker + database
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
- Week 3: add a schema registry (Confluent/Apicurio) and stop duplicating the
  `OrderCreatedEvent`/`InventoryResultEvent` classes across services
- Week 6-7: add Ingress, a Helm chart, load-test the HPA
- Week 9-11: wire the GitHub Actions workflow to a real registry and ArgoCD

## Notes on production differences
- Redpanda here runs single-node with no persistence — real deployments use
  3+ broker StatefulSets with PVCs. Postgres likewise runs without a volume,
  so data resets on `docker compose down`.
- Event classes (`OrderCreatedEvent`, `InventoryResultEvent`) are duplicated
  per service on purpose — that pain is what motivates a shared schema/contract
  in Week 3.
- The outbox relay is a 500ms poller — production systems often use CDC
  (Debezium tailing the WAL) to avoid polling. Schema is managed by Hibernate
  `ddl-auto: update`; production would use Flyway/Liquibase.
- Known gap: `inventory-service` publishes `inventory.result` inside its DB
  transaction — the same dual-write problem the outbox solved at hop 1.
  Fixing it means a second outbox; kept as a discussion point.
