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

- Documented the `order-service` API as curl examples in `README.md`
  (happy path + both failure-stock scenarios) and added
  `order-events-system/scripts/smoke-test.sh` to run those same requests
  against a live stack and assert on the HTTP response.

**Next (Week 2)**
- Add a Postgres-backed outbox table (Spring Data JPA), replace the in-memory
  `processedEventIds` set with a real idempotency mechanism.

---

## Conventions (apply from here on, every week)
- **API docs:** every new/changed REST endpoint gets a curl example in the
  relevant service's `README.md` — happy path and the meaningful failure
  paths, not just the 200 case.
- **Smoke tests:** `scripts/smoke-test.sh` grows alongside the API — one
  `check` per documented curl example. It's HTTP-contract-level only (status
  code + response shape); it does not assert on downstream Kafka/consumer
  behavior, which still needs `docker compose logs` or Redpanda Console.
  Run it after `docker compose up --build -d` before calling a week "verified."
- **Git tags:** don't wait for a week boundary — tag whenever a milestone is
  reached (a working end-to-end feature, a fixed critical bug, a stable point
  before a risky refactor), not only at week-end. Weeks that span multiple
  milestones may get more than one tag (e.g. `week-2-outbox`, `week-2-done`).

## Git checkpoints
Each finished week/milestone gets a lightweight tag on `main` (no feature
branches — low-ceremony, since this is solo work and there's no shared `main`
to protect).

| Tag | What it marks |
|-----|----------------|
| `week-1` | Week 1 lab done: producer/consumer chain verified end-to-end, build fixed, API docs + smoke test added |

To jump to any tag's state: `git checkout <tag>`. To see what a tag points to:
`git show <tag> --stat`.
