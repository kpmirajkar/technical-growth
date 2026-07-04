# 12-Week Technical Readiness Roadmap
**Principal Engineer — Event-Driven Systems, CI/CD, Kubernetes**

Goal: be able to (a) design and defend a large-scale event-driven architecture on a whiteboard, (b) stand up and operate a real Kubernetes deployment, (c) build a CI/CD pipeline that ships to it — with a working project as proof, not just talking points.

Format each week: **Theory (3-4 hrs) + Hands-on lab (5-6 hrs) + Articulation drill (1 hr — explain what you built and the trade-offs out loud, as if in an interview).**

We're starting the hands-on project today, in parallel with Week 1 theory below.

---

## Phase 1 — Event-Driven System Design (Weeks 1-4)

**Week 1: Fundamentals & Messaging Semantics**
- Pub/sub vs. message queue vs. event stream; at-least-once vs. exactly-once vs. at-most-once
- Kafka core concepts: partitions, consumer groups, offsets, replication, ISR
- Lab: stand up Kafka (Redpanda) and a Spring Boot + Spring Kafka producer/consumer — **[today's hands-on project, see below]**

**Week 2: Event-Driven Patterns**
- Event notification vs. event-carried state transfer vs. event sourcing
- CQRS, transactional outbox pattern, saga pattern (choreography vs. orchestration)
- Idempotency, dedup, ordering guarantees per partition
- Lab: extend the order system with a Postgres-backed outbox table (Spring Data JPA) and an idempotent `@KafkaListener`

**Week 3: Schema & Contracts at Scale**
- Schema registry (Avro/Protobuf), schema evolution, backward/forward compatibility
- Consumer lag, backpressure, dead-letter queues, poison messages
- Lab: add Confluent Schema Registry + Avro (via `kafka-avro-serializer` and Spring Kafka's `KafkaAvroSerializer`), replace the duplicated Java event records with a generated schema class, simulate a breaking schema change and handle it

**Week 4: Scaling & Failure Modes**
- Partition strategy for scale, rebalancing storms, exactly-once processing (Kafka transactions)
- Multi-region/multi-cluster patterns, replay & reprocessing strategy
- Articulation drill: design a large-scale order/payment system end-to-end on a whiteboard (write it up as a 1-pager — this becomes an interview artifact)

---

## Phase 2 — Kubernetes (Weeks 5-8)

**Week 5: Core Objects**
- Pods, Deployments, ReplicaSets, Services (ClusterIP/NodePort/LoadBalancer), ConfigMaps, Secrets
- Lab: containerize each Spring Boot service (multi-stage Maven → JRE Docker build) and deploy onto a local cluster (kind or minikube)

**Week 6: Networking & Config**
- Ingress, Services deep dive, DNS, namespaces, resource requests/limits, probes (liveness/readiness/startup)
- Lab: add Ingress + Spring Boot Actuator health-group probes (`/actuator/health/liveness`, `/readiness`) to each service; break a probe on purpose and observe self-healing

**Week 7: Scaling & State**
- HPA/VPA, StatefulSets, PersistentVolumes/Claims, Helm charts
- Lab: package the project as a Helm chart, add an HPA on the consumer service (scaling on CPU or, better, consumer-lag via Prometheus + KEDA), load-test it

**Week 8: Operations**
- Observability (Prometheus + Grafana via Micrometer/Actuator metrics), logging (structured JSON logs with Logback, log aggregation), RBAC basics, rolling updates vs. blue/green vs. canary at the k8s level
- Articulation drill: explain how you'd debug a pod in CrashLoopBackOff and a service with intermittent 502s — talk through your diagnostic steps

---

## Phase 3 — CI/CD (Weeks 9-12)

**Week 9: Pipeline Fundamentals**
- Build/test/package/deploy stages, trunk-based dev vs. GitFlow, artifact versioning, container image scanning
- Lab: GitHub Actions pipeline that runs `mvn verify` (JUnit 5 + Testcontainers for a real embedded-Kafka integration test), then builds/pushes images for each service

**Week 10: GitOps**
- Push-based vs. pull-based deployment, ArgoCD or Flux, declarative desired state
- Lab: wire ArgoCD to auto-deploy the Helm chart from Week 7 on every merge

**Week 11: Progressive Delivery**
- Canary, blue/green, feature flags, automated rollback triggers, deployment strategies for stateful vs. stateless services
- Lab: implement a canary rollout for one service using Argo Rollouts or a manual traffic-split approach

**Week 12: Integration & Interview Prep**
- Tie the full pipeline together: commit → CI → image → GitOps → k8s → observability feedback loop
- Mock system design interviews (2-3 sessions) covering event-driven design, k8s failure scenarios, CI/CD trade-offs
- Polish the project as a portfolio piece (README, architecture diagram, write-up of design decisions)

---

## What "done" looks like at Week 12
- A working, documented event-driven system (order → inventory → notification) running on Kubernetes, deployed via a GitOps pipeline, with schema evolution, retries/DLQ, autoscaling, and canary releases all demonstrated.
- Three 1-pagers you can talk through in an interview: event-driven design trade-offs, k8s failure diagnosis, CI/CD pipeline design.
- Comfort explaining *why*, not just *how* — trade-offs, failure modes, what you'd do differently at 10x scale.

## Weekly cadence suggestion
- Given 22+ years of experience, skip 101-level explanations — go straight to trade-offs and failure modes; use official docs/RFCs over tutorials where possible.
- Keep every lab in the same project repo so it compounds into one strong portfolio piece rather than 12 disconnected exercises.
- Toolchain default: Java 21 + Spring Boot 3 + Spring Kafka, Maven, JUnit 5 + Testcontainers for tests, Micrometer/Actuator for metrics — matching the `order-events-system` project already scaffolded in this folder.
