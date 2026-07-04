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
- Documented the `order-service` API as curl examples in `README.md`
  (happy path + both failure-stock scenarios) and added
  `order-events-system/scripts/smoke-test.sh` to run those same requests
  against a live stack and assert on the HTTP response.

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
- `inventory.result` was published with no key, breaking per-customer ordering
  past the first hop — and every topic was left to auto-create with 1
  partition anyway, so keying had nothing to demonstrate. Fixed by keying the
  `inventory.result` publish by `customer_id` (matching `orders.created`) and
  adding explicit `NewTopic` beans (3 partitions, replicas 1) for all three
  topics. `orders.created.dlq`'s partition count has to match
  `orders.created`'s, since `DeadLetterPublishingRecoverer` routes to the same
  partition number on the DLQ topic.
- The GitHub Actions workflow was dead code: it lived at
  `order-events-system/.github/workflows/`, but GitHub only discovers
  workflows at the *repo root* — so CI had never run on any push. Moved it to
  `.github/workflows/`, prefixed `working-directory`/build contexts with
  `order-events-system/`, and gave the job `packages: write` permission so
  `GITHUB_TOKEN` can push to ghcr.io.
- Commits were authored as `kpmirajkar@Krishnas-MacBook-Pro.local` (git's
  hostname fallback), so GitHub couldn't attribute them. Set global
  `user.name`/`user.email`; earlier pushed commits keep the old identity.
- Repo hygiene: untracked the committed `.idea/` files (gitignore alone
  doesn't untrack), deleted the leftover Python-draft files
  (`app.py`/`requirements.txt`/`__pycache__`), and dropped the README
  paragraph that explained them.
- Added a warning comment on the consumers' `replicas: 1` in
  `k8s/03-consumers.yaml`: scaling past 1 replica before the Week 2 outbox
  forks the in-memory stock table and breaks idempotency.

**Verified**
- `docker compose up --build` — all 5 containers healthy.
- Happy path: `POST /orders` (WIDGET-1, qty 2) → `InventoryReserved` →
  notification email logged.
- Failure path: `POST /orders` (WIDGET-2, qty 10 > 5 in stock) →
  `InventoryFailed` (`insufficient_stock`) → failure email logged.
- Partitioning: sent repeated orders for the same `customer_id`s and used
  `rpk topic consume` to confirm each customer lands on the same partition on
  both `orders.created` and `inventory.result`.
- CI: after relocating the workflow, run #1 (the first ever) went green —
  all three `mvn verify` matrix jobs passed and all three images pushed to
  ghcr.io. The `week-1` tag predates this fix on purpose; the CI fix is
  part of `main`'s history, and moving a published tag a third time wasn't
  worth it.
- Learning-mode gate: image publish + deploy now run only on manual
  `workflow_dispatch`, not on every push to `main`. Pushes/PRs still get
  build + test. (No billing risk existed — public repo, so Actions minutes
  are free, and the deploy job is an echo stub with no cloud behind it —
  but pushing 3 images per commit was pointless clutter in learning mode.)
- Upgraded Java 17 → 21 across all poms, Dockerfiles, CI, and docs. 21 over
  25 (the newest LTS) because Spring Boot 3.3.2 doesn't officially support
  25 — the path to 25 is a deliberate Boot 3.5+ bump later. Local sdkman JDK
  is already 25, which compiles `--release 21` fine. Verified: full stack
  rebuilt on Temurin 21 images, smoke test green, events flowing end-to-end.

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
