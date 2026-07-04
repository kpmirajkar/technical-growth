# Progress Log

Companion to [Roadmap.md](Roadmap.md). One section per week: what actually shipped,
what broke and how it was fixed, and what's next. Each week's checkpoint is also
tagged in git (`git tag`) so you can check out the exact state at any point —
see "Git checkpoints" at the bottom.

---

## Week 1 — Fundamentals & Messaging Semantics
**Status:** Done — 2026-07-04

**Shipped**
- `order-events-system`: Spring Boot 3 + Spring Kafka producer/consumer chain —
  `order-service` → Kafka (Redpanda) → `inventory-service` → `notification-service`.
- Idempotent consumption (dedup by `event_id`), `ErrorHandlingDeserializer` +
  `DeadLetterPublishingRecoverer` for the inventory consumer.
- Dockerized all three services, `docker-compose.yml` wiring Redpanda + Redpanda
  Console + the three services.
- GitHub Actions CI/CD skeleton (`.github/workflows/ci-cd.yaml`): `mvn verify`
  per service, then build/push image on `main`.

**Issues hit & fixed**
- `consumer_inventory` and `consumer_notification` failed to build: both only
  depended on `spring-boot-starter` (not `-web`), so Jackson wasn't on the
  classpath even though the event records use `@JsonProperty` and Spring Kafka's
  `JsonSerializer`/`JsonDeserializer` need it. Fixed by adding an explicit
  `jackson-databind` dependency to both `pom.xml`s. Verified via `mvn -B verify`
  (the exact CI command) and `docker build` for both.
- `git push` failed (`HTTP 400`, RPC failure) because the repo had no
  `.gitignore` — the initial commit accidentally included `target/` build
  output (two ~32 MB fat jars) and `__pycache__`. Added a `.gitignore`, untracked
  the artifacts, amended the not-yet-pushed commit, pushed clean.

**Verified**
- `docker compose up --build` — all 5 containers healthy.
- Happy path: `POST /orders` (WIDGET-1, qty 2) → `InventoryReserved` →
  notification email logged.
- Failure path: `POST /orders` (WIDGET-2, qty 10 > 5 in stock) →
  `InventoryFailed` (`insufficient_stock`) → failure email logged.

**Next (Week 2)**
- Add a Postgres-backed outbox table (Spring Data JPA), replace the in-memory
  `processedEventIds` set with a real idempotency mechanism.

---

## Git checkpoints
Each finished week gets a lightweight tag on `main` (no feature branches —
low-ceremony, since this is solo work and there's no shared `main` to protect).

| Week | Tag |
|------|-----|
| 1 | `week-1` |

To jump to any week's state: `git checkout week-N`. To see what a tag points to:
`git show week-N --stat`.
