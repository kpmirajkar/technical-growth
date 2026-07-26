# Week 1 — Messaging Semantics & Kafka Mechanics (study sheet)

Interview one-pager: the mental models that generate correct answers, plus the
trap hiding inside each one. Anchored to `order-events-system` — "I built this
and hit this exact bug" is both the best retention device and the best
interview answer.

## 0. Queue vs. pub/sub vs. event stream — who owns replay and position?

The real differentiator between messaging models isn't fan-out; it's *who
owns the read position and whether history can be replayed*:

- **Message queue** (SQS, RabbitMQ queue): delete-on-ack, competing
  consumers. Good for task distribution / load leveling. No replay, no
  fan-out — once one consumer takes it, it's gone.
- **Pub/sub** (SNS, JMS topics): fans out, but *ephemeral* — a subscriber
  that's down or not yet subscribed at publish time misses the message
  (absent pre-provisioned durable subscriptions). No rewind.
- **Event stream** (Kafka/Redpanda): durable, ordered, append-only log with
  retention. Each consumer group tracks its *own* offset and reads the full
  history at its own pace. **Replay + independent consumption position** is
  the differentiator, not fan-out.

```mermaid
flowchart TB
    subgraph queue["Message queue — competing consumers, delete on ack"]
        direction LR
        QP["producer"] --> Q[["queue"]]
        Q -->|"msg 1"| QA["consumer A"]
        Q -->|"msg 2"| QB["consumer B"]
    end
    subgraph pubsub["Pub/sub — fan-out, but ephemeral"]
        direction LR
        PP["producer"] --> PT(["topic"])
        PT --> PA["subscriber A"]
        PT --> PB["subscriber B is down<br/>misses it forever"]
    end
    subgraph stream["Event stream — durable log, consumer-owned offsets"]
        direction LR
        SP["producer"] --> SL["log: 0 1 2 3 4 5 6 ..."]
        SL --> SA["group A at offset 6"]
        SL --> SB["group B at offset 2<br/>catching up after downtime"]
        SL --> SC["new group C<br/>replays from 0"]
    end
```

**Trap:** "Kafka is pub/sub" undersells it. The log + consumer-owned offsets
is what enables: catching up after downtime (notification-service down an
hour just resumes from its committed offset), adding a brand-new consumer
that reads history from the beginning, and reprocessing after a bug fix.
None of those exist in classic pub/sub.

## 1. Delivery semantics are decided by operation order, not configuration

The at-most-once / at-least-once / exactly-once taxonomy reduces to one
question: *in what order do you process a message and commit its offset?*

- Commit-first → **at-most-once** (crash loses work)
- Process-first → **at-least-once** (crash duplicates work)
- Atomically both → **exactly-once**

**Trap:** "exactly-once" is exactly-once *within Kafka's log*. The moment a
consumer touches the outside world (email API, external HTTP call), no Kafka
transaction protects you — you need idempotency at that boundary.

The senior framing: real systems run **at-least-once + idempotent consumers =
effectively-once** — exactly what the `event_id` dedup in `InventoryConsumer`
implements.

Keep the two dedup layers distinct:
- **Broker-level idempotent producer** (`producer_id` + sequence) dedups
  *retried writes* — e.g. an ack lost over the network after the write landed.
- **App-level dedup by business key** handles *reprocessing* — crashes,
  rebalances.

Different failure modes; both needed.

## 2. Ordering = partition key choice, nothing else

Kafka guarantees order **within a partition only**. Every ordering discussion
is really a key-choice discussion, and the key simultaneously sets three
things: ordering scope (per-customer here), parallelism grain, and hotspot
risk (one whale customer = one hot partition).

```mermaid
flowchart TB
    subgraph keyed["Keyed by customer_id — per-customer order preserved"]
        direction LR
        KA["cust-a: e1 e2 e3"] -->|"hash(cust-a) % 3"| KP2["partition 2<br/>e1 e2 e3 in order"]
        KB["cust-b: e1 e2"] -->|"hash(cust-b) % 3"| KP1["partition 1<br/>e1 e2 in order"]
    end
    subgraph unkeyed["Unkeyed — the hop-2 bug we shipped"]
        direction LR
        UA["cust-a: e1 e2 e3"] --> UP0["partition 0 — e1"]
        UA --> UP1["partition 1 — e2"]
        UA --> UP2["partition 2 — e3"]
    end
```

Round-robin across partitions means three consumers process one customer's
events concurrently, in any order. Nothing errors; the guarantee just quietly
stops being true.

**Two traps:**
- The key must survive *every hop*. Lived this one: `inventory.result` was
  published unkeyed, silently breaking per-customer ordering at hop two.
- Changing partition count remaps `hash(key) % N` for all existing keys —
  ordering continuity breaks silently. Partition count is effectively a
  one-way, day-one decision (you can add, never remove).

## 3. A consumer group is one logical subscriber

Within a group, partitions are divided among members — so **partition count
is the parallelism ceiling** (3 partitions = max 3 useful `inventory-service`
replicas). Across groups, each group independently reads everything — that's
the fan-out.

```mermaid
flowchart LR
    subgraph topic["orders.created — 3 partitions"]
        TP0["p0"]
        TP1["p1"]
        TP2["p2"]
    end
    TP0 --> R1["inventory replica 1"]
    TP1 --> R2["inventory replica 2"]
    TP2 --> R3["inventory replica 3"]
    IDLE["replicas 4-10:<br/>assigned nothing, burn money"]
    topic -.->|"separate group,<br/>reads everything independently"| NOTIF["notification-service"]
```

- Offsets are per `(group, partition)`, stored in `__consumer_offsets`.
- `auto-offset-reset: earliest` only applies when *no committed offset
  exists*. It is **not** a replay button — changing it does nothing for an
  existing group.

**The trap interviewers love:** rebalances trigger not just on real
membership changes but on *perceived death* — a listener exceeding
`max.poll.interval.ms` (default 5 min) looks dead, triggers a rebalance,
pauses consumption, and reprocesses in-flight work. **Slow processing
therefore causes duplicates.** Cooperative-sticky assignment (the modern
default) softens but doesn't eliminate the pause.

## 4. Durability is an acknowledgment contract — `acks=all` alone is paper

`acks=all` means "acknowledged by the *current ISR*". If the ISR has shrunk
to just the leader, that's one copy.

Recite the trio together — any one alone is meaningless:
**RF=3 + `min.insync.replicas=2` + `acks=all`** → one broker dies with zero
loss and writes continue; a second death blocks producers — deliberately
trading availability for consistency.

```mermaid
flowchart TB
    subgraph safe["RF=3 + min.insync.replicas=2 + acks=all"]
        direction LR
        P1["producer"] --> L1["leader"]
        L1 --> FA["follower<br/>in ISR"]
        L1 --> FB["follower<br/>in ISR"]
        FA -.->|"ack once 2 replicas hold it"| P1
    end
    subgraph today["This project today: RF=1"]
        direction LR
        P2["producer<br/>acks=all"] --> L2["leader<br/>ISR = itself only"]
        L2 -.->|"acked after 1 copy"| P2
        L2 -->|"disk dies"| GONE["order is GONE<br/>customer already saw HTTP 200"]
    end
```

Left: one broker can die with zero loss and writes continue; a *second* loss
drops the ISR below 2 and producers **block** — availability traded for
consistency, deliberately. Right: `acks=all` with a single replica is
`acks=1` wearing a costume.

This project is the honest counterexample: `acks: all` is configured, but
topics are `replicas(1)`, so today it's equivalent to `acks=1`. Saying that
unprompted is a strong signal.

**Unclean leader election** is the explicit availability-vs-consistency
dial: allow a lagging (out-of-ISR) replica to become leader and you can lose
*already-acknowledged* writes. Safe default: off.

## 5. Failure handling: the poison message is the central villain

One undeserializable record, naively handled, blocks its partition forever.
The pattern: *bounded* retries → DLQ — which is what `DefaultErrorHandler` +
`DeadLetterPublishingRecoverer` does here (2 retries, 1s backoff, then
`orders.created.dlq`).

```mermaid
flowchart LR
    REC["record arrives"] --> DES{"ErrorHandlingDeserializer"}
    DES -->|"cannot deserialize<br/>(not retryable — skips retries)"| DLQ[("orders.created.dlq")]
    DES -->|"ok"| LIS{"listener"}
    LIS -->|"throws"| RETRY["retry x2, 1s backoff"]
    RETRY -->|"still failing"| DLQ
    LIS -->|"ok"| DONE["processed, offset committed"]
```

Note the asymmetry: a malformed payload goes **straight** to the DLQ —
`DefaultErrorHandler` classifies `DeserializationException` as fatal, since
retrying a byte sequence that will never parse is pointless. Only *listener*
exceptions get the backoff.

Nuances to have ready:
- Deserialization failures happen *before* listener code runs — that's why
  `ErrorHandlingDeserializer` must wrap the deserializer itself.
- A DLQ without a monitoring-and-replay story is a write-only graveyard.
  "What happens to messages after they land in the DLQ?" is the standard
  follow-up.
- **Consumer lag** is the health metric that ties it together: lag growing =
  consuming slower than producing = your first page.

## The meta-frame

Almost every interview question in this domain is one of four trade-offs
wearing a costume:

1. **Latency vs. durability** — acks
2. **Throughput/parallelism vs. ordering** — partitions and keys
3. **Availability vs. consistency** — min.ISR, unclean leader election
4. **Simplicity vs. delivery strength** — at-least-once + idempotency vs.
   transactions

Name which trade-off the question is really about, then say what breaks when
each component fails — that's the altitude being tested.
